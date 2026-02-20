from pathlib import Path
from typing import Optional, Literal, Dict, Any, Tuple
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    PydanticBaseSettingsSource,
)
from pydantic import Field
from pydantic.fields import FieldInfo
import yaml
import tomllib

CONFIG_DIR = Path.home() / ".snowmin"
CONFIG_FILE = CONFIG_DIR / "config.yaml"


def get_toml_config_path() -> Path:
    """Get the path to the TOML configuration file"""
    return Path.home() / ".snowflake" / "connections.toml"


def load_snowflake_connection(profile: Optional[str] = None) -> Dict[str, Any]:
    """
    Load Snowflake connection configuration from ~/.snowflake/connections.toml.
    Returns a dictionary of connection parameters.
    """
    result = {}

    toml_path = get_toml_config_path()
    if toml_path.exists():
        try:
            with open(toml_path, "rb") as f:
                data = tomllib.load(f)

            connection_name = profile or "default"
            if connection_name in data:
                result.update(data[connection_name])
                return result

            if profile and profile not in data:
                raise ValueError(f"Connection '{profile}' not found in {toml_path}")

        except tomllib.TOMLDecodeError as e:
            raise ValueError(f"Error parsing {toml_path}: {e}")

    return result


class EnvironmentConfigSettingsSource(PydanticBaseSettingsSource):
    """
    Custom settings source that loads from environment-specific config file.
    Looks for ./snowmin_{env}.yaml where {env} comes from SNOWMIN_ENV or defaults to 'dev'.
    """

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        # Get environment name from env var or default to 'dev'
        import os

        env_name = os.getenv("SNOWMIN_ENV", "dev")
        env_config_file = Path.cwd() / f"snowmin_{env_name}.yaml"

        if not env_config_file.exists():
            return None, field_name, False

        try:
            with open(env_config_file, "r") as f:
                config_data = yaml.safe_load(f) or {}

            # Handle alias for schema_name -> schema
            if field_name == "schema_name" and "schema" in config_data:
                field_value = config_data.get("schema")
            else:
                field_value = config_data.get(field_name)

            return field_value, field_name, False
        except Exception:
            return None, field_name, False

    def prepare_field_value(
        self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool
    ) -> Any:
        return value

    def __call__(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(
                field, field_name
            )
            field_value = self.prepare_field_value(
                field_name, field, field_value, value_is_complex
            )
            if field_value is not None:
                d[field_key] = field_value
        return d


class GenericConfigSettingsSource(PydanticBaseSettingsSource):
    """
    Custom settings source that loads from generic config file ~/.snowmin/config.yaml.
    """

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        if not CONFIG_FILE.exists():
            return None, field_name, False

        try:
            with open(CONFIG_FILE, "r") as f:
                config_data = yaml.safe_load(f) or {}

            # Handle alias for schema_name -> schema
            if field_name == "schema_name" and "schema" in config_data:
                field_value = config_data.get("schema")
            else:
                field_value = config_data.get(field_name)

            return field_value, field_name, False
        except Exception:
            return None, field_name, False

    def prepare_field_value(
        self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool
    ) -> Any:
        return value

    def __call__(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(
                field, field_name
            )
            field_value = self.prepare_field_value(
                field_name, field, field_value, value_is_complex
            )
            if field_value is not None:
                d[field_key] = field_value
        return d


class Settings(BaseSettings):
    """
    Snowmin Configuration Settings.

    Priority order (highest to lowest):
    1. CLI parameters (init_settings)
    2. Environment variables (SNOWMIN__*)
    3. Environment-specific config file (./snowmin_{env}.yaml)
    4. Generic config file (~/.snowmin/config.yaml)
    """

    model_config = SettingsConfigDict(
        env_prefix="SNOWMIN__",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    # Connection reference
    connection: Optional[str] = Field(
        None, description="Connection profile name from ~/.snowflake/connections.toml"
    )

    # Connection override fields (optional - for CLI/env overrides)
    account: Optional[str] = Field(None, description="Snowflake account identifier")
    user: Optional[str] = Field(None, description="Snowflake username")
    password: Optional[str] = Field(
        None, description="Snowflake password (optional if using other auth)"
    )
    role: Optional[str] = Field(None, description="Default role to use")
    warehouse: Optional[str] = Field(None, description="Default warehouse to use")
    database: Optional[str] = Field(None, description="Default database to use")
    schema_name: Optional[str] = Field(
        None, alias="schema", description="Default schema to use"
    )
    private_key_file: Optional[str] = Field(
        None, description="Path to private key file for key-pair authentication"
    )
    private_key_passphrase: Optional[str] = Field(
        None, description="Passphrase for encrypted private key"
    )

    # Tool Settings
    state_backend: Literal["stateless"] = "stateless"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Customize the priority order of settings sources.

        Priority (highest to lowest):
        1. init_settings - CLI parameters passed to Settings()
        2. env_settings - Environment variables (SNOWMIN__*)
        3. EnvironmentConfigSettingsSource - ./snowmin_{env}.yaml
        4. GenericConfigSettingsSource - ~/.snowmin/config.yaml
        """
        return (
            init_settings,
            env_settings,
            EnvironmentConfigSettingsSource(settings_cls),
            GenericConfigSettingsSource(settings_cls),
        )

    def save(self):
        """Save current settings to config file."""
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)

        # Dump model to dict, exclude None values to keep file clean
        data = self.model_dump(mode="json", exclude_none=True)

        with open(CONFIG_FILE, "w") as f:
            yaml.dump(data, f)


def get_merged_connection_config(
    settings: Settings, cli_overrides: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Merge connection configuration with proper priority:
    1. CLI overrides (highest priority)
    2. Settings (from all sources: CLI params > env vars > env config > generic config)
    3. Connection profile from connections.toml (lowest priority)

    Returns a dictionary ready for ConnectionManager.
    """
    cli_overrides = cli_overrides or {}

    # Start with connection profile from TOML
    connection_profile = settings.connection or cli_overrides.get("connection")
    conn_config = load_snowflake_connection(connection_profile)

    # Apply settings overrides (already merged from all sources by Pydantic)
    settings_dict = settings.model_dump(
        exclude_none=True, exclude={"state_backend", "connection"}
    )
    for key, value in settings_dict.items():
        if value is not None:
            conn_config[key] = value

    # Apply CLI overrides (highest priority)
    for key, value in cli_overrides.items():
        if value is not None and key != "connection":
            conn_config[key] = value

    # Normalize 'username' to 'user' if present
    if "username" in conn_config and "user" not in conn_config:
        conn_config["user"] = conn_config.pop("username")

    return conn_config


# Global settings instance - lazy loaded
def get_settings(**kwargs) -> Settings:
    """
    Get settings instance.

    Args:
        **kwargs: Optional keyword arguments to pass to Settings (CLI overrides)

    Returns:
        Settings instance with values loaded from all sources
    """
    try:
        return Settings(**kwargs)
    except Exception as e:
        # If loading fails (e.g. missing required fields), we might return a partial or let it raise
        # For CLI usage, we might want to handle this gracefully
        raise e
