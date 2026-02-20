from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from snowmin.core.state import Resource


class ResourceRegistry:
    _resources: Dict[str, "Resource"] = {}

    @classmethod
    def register(cls, resource: "Resource"):
        """Register a resource instance."""
        # Key by unique identifier: type.name (e.g. warehouse.compute_wh)
        key = resource.identifier
        if key in cls._resources:
            # Overwrite allowed?
            pass
        cls._resources[key] = resource

    @classmethod
    def get_all(cls) -> List["Resource"]:
        return list(cls._resources.values())

    @classmethod
    def clear(cls):
        cls._resources = {}
