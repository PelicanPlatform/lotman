#include <nlohmann/json-schema.hpp>

using json = nlohmann::json;
using json_validator = nlohmann::json_schema::json_validator;


namespace lotman_schemas {

json new_lot_schema = R"(
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "new lot obj",
    "additionalProperties": false,
    "properties": {
        "lot_name": {
            "description": "Lot Name",
            "type": "string",
            "minLength": 1
        },
        "owner": {
            "description": "Entity who owns the lot",
            "type": "string",
            "minLength": 1
        },
        "parents": {
            "description": "The names of parent lots",
            "type": "array",
            "items": {
                "type": "string"
            },
            "minItems":1
        },
        "children": {
            "description": "The names of children lots",
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "paths": {
            "description": "paths array",
            "type": "array",
            "items": {
                "type": "object",
                "description": "path object",
                "additionalProperties": false,
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1
                    },
                    "recursive": {
                        "type": "boolean"
                    }
                },
                "required": ["path", "recursive"]
            }
        },
        "management_policy_attrs": {
            "description": "management policy attributes",
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "dedicated_GB": {
                    "description": "Amount of dedicated storage attributed to the lot, in GB",
                    "type": "number",
                    "minimum": 0
                },
                "opportunistic_GB": {
                    "description": "Amount of opportunistic storage attributed to the lot, in GB",
                    "type": "number",
                    "minimum": 0
                },
                "max_num_objects": {
                    "description": "Max number of objects a lot should have",
                    "type": "number",
                    "minimum": 0,
                    "multipleOf": 1
                },
                "creation_time": {
                    "description": "Creation time of the lot, in ms since Unix epoch",
                    "type": "number",
                    "minimum": 0,
                    "multipleOf": 1
                },
                "expiration_time": {
                    "description": "Expiration time of the lot, in ms since Unix epoch",
                    "type": "number",
                    "minimum": 0,
                    "multipleOf": 1
                },
                "deletion_time": {
                    "description": "Deletion time of the lot, in ms since Unix epoch",
                    "type": "number",
                    "minimum": 0,
                    "multipleOf": 1
                }
            },
            "required": ["dedicated_GB", "opportunistic_GB", "max_num_objects", "creation_time", "expiration_time", "deletion_time"]
        }
    },
    "required": ["lot_name", "owner", "parents", "management_policy_attrs"]
}
)"_json;

json lot_update_schema = R"(
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "update obj",
    "additionalProperties": false,
    "properties": {
        "lot_name": {
            "description": "Name of lot being modified",
            "type": "string",
            "minLength": 1
        },

        "owner": {
            "description": "New entity to whom the lot should belong",
            "type": "string",
            "minLength": 1
        },

        "parents": {
            "description": "Array of parent updates",
            "type": "array",
            "items": {
                "type": "object",
                "description": "parent update object",
                "additionalProperties": false,
                "properties": {
                    "current": {
                        "type": "string",
                        "minLength": 1
                    },
                    "new": {
                        "type": "string",
                        "minLength": 1
                    }
                },
                "required": ["current", "new"]
            }
        },
        
        "paths": {
            "description": "Array of path updates",
            "type": "array",
            "items": {
                "type": "object",
                "description": "path update object",
                "additionalProperties": false,
                "properties": {
                    "current": {
                        "description": "path to be updated",
                        "type": "string",
                        "minLength": 1
                    },
                    "new": {
                        "description": "new path",
                        "type": "string",
                        "minLength": 1
                    },
                    "recursive": {
                        "description": "whether new path is recursive",
                        "type": "boolean"
                    }
                },
                "required": ["current", "new", "recursive"]
            }
        },

        "management_policy_attrs": {
            "description": "Management policy attribute updates",
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "dedicated_GB": {
                    "description": "Amount of dedicated storage attributed to the lot, in GB",
                    "type": "number",
                    "minimum": 0
                },
                "opportunistic_GB": {
                    "description": "Amount of opportunistic storage attributed to the lot, in GB",
                    "type": "number",
                    "minimum": 0
                },
                "max_num_objects": {
                    "description": "Max number of objects a lot should have",
                    "type": "number",
                    "minimum": 0,
                    "multipleOf": 1
                },
                "expiration_time": {
                    "description": "Expiration time of the lot, in ms since Unix epoch",
                    "type": "number",
                    "minimum": 0,
                    "multipleOf": 1
                },
                "deletion_time": {
                    "description": "Deletion time of the lot, in ms since Unix epoch",
                    "type": "number",
                    "minimum": 0,
                    "multipleOf": 1
                }
            }
        }
    },
    "required": ["lot_name"]
}
)"_json;

json lot_subtractions_schema = R"(
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "subtraction obj",
    "additionalProperties": false,
    "properties": {
        "lot_name": {
            "description": "Name of lot being modified",
            "type": "string",
            "minLength": 1
        },

        "parents": {
            "description": "The names of parent lots to be removed",
            "type": "array",
            "items": {
                "type": "string"
            },
            "minItems":1
        },
        
        "paths": {
            "description": "The paths to be removed from the lot",
            "type": "array",
            "items": {
                "type": "string"
            },
            "minItems":1
        }
    },
    "required": ["lot_name"]
}
)"_json;

json lot_additions_schema = R"(
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "additions obj",
    "additionalProperties": false,
    "properties": {
        "lot_name": {
            "description": "Name of lot being modified",
            "type": "string",
            "minLength": 1
        },

        "paths": {
            "description": "New paths array",
            "type": "array",
            "items": {
                "description": "new path obj",
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1
                    },
                    "recursive": {
                        "type": "boolean"
                    }
                },
                "required": ["path", "recursive"]
            }
        }, 

        "parents": {
            "description": "Array of parents to assign to lot",
            "type": "array",
            "items": {
                "description": "Names of new parent lots",
                "type": "string",
                "minLength": 1
            }
        }
    },
    "required": ["lot_name"]
}
)"_json;

json get_policy_attrs_schema = R"(
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "get policy attrs obj",
    "additionalProperties": false,
    "properties": {
        "lot_name": {
            "description": "Name of lot whose attrs are to be obtained",
            "type": "string",
            "minLength": 1
        },

        "dedicated_GB": {
            "description": "true -> get most restrictive attr, false -> get attr recorded for lot",
            "type": "boolean"
        },

        "opportunistic_GB": {
            "description": "true -> get most restrictive attr, false -> get attr recorded for lot",
            "type": "boolean"
        },

        "max_num_objects": {
            "description": "true -> get most restrictive attr, false -> get attr recorded for lot",
            "type": "boolean"
        },

        "creation_time": {
            "description": "true -> get most restrictive attr, false -> get attr recorded for lot",
            "type": "boolean"
        },

        "expiration_time": {
            "description": "true -> get most restrictive attr, false -> get attr recorded for lot",
            "type": "boolean"
        },

        "deletion_time": {
            "description": "true -> get most restrictive attr, false -> get attr recorded for lot",
            "type": "boolean"
        }
    },
    "required": ["lot_name"]
}
)"_json;

json get_usage_schema = R"(
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "get usage obj",
    "additionalProperties": false,
    "properties": {
        "lot_name": {
            "description": "Name of lot whose attrs are to be obtained",
            "type": "string",
            "minLength": 1
        },

        "dedicated_GB": {
            "description": "true -> count children toward usage, false -> don't",
            "type": "boolean"
        },

        "opportunistic_GB": {
            "description": "true -> count children toward usage, false -> don't",
            "type": "boolean"
        },

        "total_GB": {
            "description": "true -> count children toward usage, false -> don't",
            "type": "boolean"
        },

        "num_objects": {
            "description": "true -> count children toward usage, false -> don't",
            "type": "boolean"
        },

        "GB_being_written": {
            "description": "true -> count children toward usage, false -> don't",
            "type": "boolean"
        },

        "objects_being_written": {
            "description": "true -> count children toward usage, false -> don't",
            "type": "boolean"
        }
    },
    "required": ["lot_name"]
}
)"_json;

json update_usage_schema = R"(
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "update usage obj",
    "additionalProperties": false,
    "properties": {
        "lot_name": {
            "description": "Name of lot whose attrs are to be obtained",
            "type": "string",
            "minLength": 1
        },
        "self_GB": {
            "description": "The number of GB a lot is using, not including children usage.",
            "type": "number",
            "minimum": 0
        },
        "self_objects": {
            "description": "The number of objects attributed to a lot, not including children.",
            "type": "number",
            "minimum": 0,
            "multipleOf": 1
        },
        "self_GB_being_written": {
            "description": "GB currently being written to a lot, not including children.",
            "type": "number",
            "minimum": 0
        },
        "self_objects_being_written": {
            "description": "The number of objects being written to a lot, not including children.",
            "type": "number",
            "minimum": 0,
            "multipleOf": 1
        }
    },
    "required": ["lot_name"]
}
)"_json;


/*
NOTE: This schema only validates a single top level object, but does get the array of objects 
      after "subdirs". To validate the top array of these objects, iterate through the array
      and test against each object.

TODO: Fix this so it validates the top level array instead of validating each individualobject
      in the array
*/
json update_usage_by_dir_schema = R"(
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "update usage by dir obj",
    "additionalProperties": false,
    "properties": {
        "path": {
            "description": "The path/object name.",
            "type": "string",
            "minLength": 1
        },
        "size_GB": {
            "description": "The size of the resource.",
            "type": "number",
            "minimum": 0
        },
        "num_obj": {
            "description": "The number of objects, if the resource itself is not an object.",
            "type": "number",
            "minimum": 0,
            "multipleOf": 1
        },
        "includes_subdirs": {
            "description": "Whether or not any subdirs are counted toward the size_GB or num_obj values.",
            "type": "boolean"
        },
        "subdirs": {
            "description": "JSON of any of the subdirs, required if include_subdirs is true.",
            "type": "array",
            "items": {
                "$ref": "#"
            }
        }
    },
    "required": ["path", "includes_subdirs"]
}
)"_json;

} // namespace schemas
