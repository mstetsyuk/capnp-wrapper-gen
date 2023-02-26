#!/usr/bin/env python3

import capnp
import sys

SPEC_FILE = 'test/trangequery.capnp'
CAPNP_BASIC_TYPES = {
    'void': 'void',
    'bool': 'bool',
    'int8': 'int8_t',
    'int16': 'int16_t',
    'int32': 'int32_t',
    'int64': 'int64_t',
    'uint8': 'uint8_t',
    'uint16': 'uint16_t',
    'uint32': 'uint32_t',
    'uint64': 'uint64_t',
    'float32': 'float',
    'float64': 'double',
    'text': 'std::string',
    'data': 'std::string',
}
BASE_NAMESPACE = 'NKikimrCapnProto_'

def cap(s):
    return s[0].upper() + s[1:]

def low(s):
    return s[0].lower() + s[1:]

class Parser:
    def __init__(self):
        self.struct_ids = {} # struct id -> struct name
        self.enum_ids = {} # enum id -> enum name
        self.nodes = {} # node name -> ((field name -> field type) | enumerants)

    @staticmethod
    def get_struct_id(struct) -> int:
        return struct.schema.get_proto().id

    @staticmethod
    def get_enum_id(enum) -> int:
        return enum.schema.node.id

    def get_list_type(self, list_) -> int:
        if list_.proto.slot.type.list.elementType._which_str() == 'struct':
            return self.struct_ids[list_.proto.slot.type.list.elementType.struct.typeId]
        return list_.proto.slot.type.list.elementType._which_str()

    def parse_struct(self, fields):
        res = {} # field name -> field type
        for name, field in fields.items():
            type_ =  field.proto.slot.type.which()
            if type_ == 'struct':
                type_ = self.struct_ids[self.get_struct_id(field)]
            elif type_ == 'enum':
                type_ = self.enum_ids[self.get_enum_id(field)]
            elif type_ == 'list':
                type_ = f'List({self.get_list_type(field)})'
            res[cap(name)] = type_

        return res

    def parse(self, file_path: str):
        schema = capnp.load(file_path)
        for name, module in schema.__dict__.items():
            if isinstance(module, capnp.lib.capnp._StructModule):
                self.struct_ids[self.get_struct_id(module)] = name
                self.nodes[name] = self.parse_struct(module.schema.fields)
            if isinstance(module, capnp.lib.capnp._EnumModule):
                self.enum_ids[self.get_enum_id(module)] = name
                self.nodes[name] = [cap(enumerant) for enumerant in module.schema.enumerants.keys()][1:]
        return self.nodes


class Generator:
    def __init__(self, nodes):
        self.nodes = nodes
        self.enum_names = [node for node, content in self.nodes.items() if type(content) is list]
        self.struct_names = [node for node, content in self.nodes.items() if type(content) is dict]

    def is_enum(self, node):
        return node in self.nodes and type(self.nodes[node]) is list

    def is_struct(self, node):
        return node in self.nodes and type(self.nodes[node]) is dict

    def is_basic_type(self, node):
        return node in CAPNP_BASIC_TYPES

    @staticmethod
    def generate_reader_constructors(name):
        return [f"Reader({BASE_NAMESPACE}::{name}::Reader r) : {BASE_NAMESPACE}::{name}::Reader(r) {{}}", "Reader() = default;"]

    @staticmethod
    def generate_builder_constructors(name):
        return [f"Builder({BASE_NAMESPACE}::{name}::Builder b) : {BASE_NAMESPACE}::{name}::Builder(b), Reader(b.asReader()) {{}}"]

    @staticmethod
    def generate_builder_operators():
        return [
            "Builder* operator->() { return this; }",
            "Builder& operator*() { return *this; }",
        ]

    @staticmethod
    def generate_capnp_base_reader_method(name):
        return f"const {BASE_NAMESPACE}::{name}::Reader& GetCapnpBase() const {{ return *this; }}"

    @staticmethod
    def generate_capnp_base_builder_method(name):
        return f"const {BASE_NAMESPACE}::{name}::Builder& GetCapnpBase() const {{ return *this; }}"

    def generate_using_builder_methods(self, name, fields):
        sep = '\n' + 8 * ' '
        usings = [f"using {BASE_NAMESPACE}::{name}::Builder::get{field};" for field, type_ in fields.items() if self.is_struct(type_)]
        return sep.join(usings)

    def generate_has_methods(self, struct_name):
        struct = [f"bool Has{name}() const {{ return has{name}(); }}" for name, type_ in self.nodes[struct_name].items() if self.is_struct(type_)]
        enum = [f"bool Has{name}() const {{ return get{name}() != {BASE_NAMESPACE}::{type_}::NOT_SET; }}" for name, type_ in self.nodes[struct_name].items() if self.is_enum(type_)]
        basic = [f"bool Has{name}() const {{ return get{name}() != 0; }}" for name, type_ in self.nodes[struct_name].items() if self.is_basic_type(type_)]
        return struct + enum + basic

    def generate_get_methods(self, struct_name):
        fields = self.nodes[struct_name].items()

        template = "{type_} Get{name}() const {{ return get{name}(); }}"
        basic = [template.format(name=name, type_=CAPNP_BASIC_TYPES[type_]) for name, type_ in fields if self.is_basic_type(type_)]
        struct = [template.format(name=name, type_=f"{type_}::Reader") for name, type_ in fields if self.is_struct(type_)]

        enum_template = "{type_} Get{name}() const {{ return static_cast<{type_}>(static_cast<size_t>(get{name}()) - 1); }}"
        enum = [enum_template.format(name=name, type_=type_) for name, type_ in fields if self.is_enum(type_)]

        return basic + struct + enum

    def generate_set_methods(self, struct_name):
        fields = self.nodes[struct_name].items()

        basic_template = "void Set{name}(const {type_}& value) {{ return set{name}(value); }}"
        basic = [basic_template.format(name=name, type_=CAPNP_BASIC_TYPES[type_]) for name, type_ in fields if self.is_basic_type(type_)]

        struct_template = "void Set{name}(const {type_}::Reader& value) {{ return set{name}(value.GetCapnpBase()); }}"
        struct = [struct_template.format(name=name, type_=type_) for name, type_ in fields if self.is_struct(type_)]

        enum_template = "void Set{name}(const {type_}& value) {{ return set{name}(static_cast<{base_ns}::{type_}>(static_cast<size_t>(value) + 1)); }}"
        enum = [enum_template.format(name=name, type_=type_, base_ns=BASE_NAMESPACE) for name, type_ in fields if self.is_enum(type_)]

        return basic + struct + enum

    def generate_mutable_methods(self, struct_name):
        return [f"{type_}::Builder Mutable{name}() {{ return get{name}(); }}" for name, type_ in self.nodes[struct_name].items() if self.is_struct(type_)]

    def generate_reader(self, name, fields):
        sep = '\n' + 8 * ' '
        methods = self.generate_reader_constructors(name) + \
                  self.generate_get_methods(name) + \
                  self.generate_has_methods(name) + \
                  [self.generate_capnp_base_reader_method(name)]
        return sep.join(methods)

    def generate_builder(self, name, fields):
        sep = '\n' + 8 * ' '
        methods = self.generate_builder_constructors(name) + \
                  self.generate_builder_operators() + \
                  self.generate_set_methods(name) + \
                  self.generate_mutable_methods(name) + \
                  [self.generate_capnp_base_builder_method(name)]
        return sep.join(methods)

    def generate_struct(self, name, fields):
        code = \
            f"""
struct {name} {{
    struct Reader : private {BASE_NAMESPACE}::{name}::Reader {{
    public:
        {self.generate_reader(name, fields)}
    }};
    
    struct Builder : private {BASE_NAMESPACE}::{name}::Builder, public Reader {{
    private:
        {self.generate_using_builder_methods(name, fields)}
    public:
        {self.generate_builder(name, fields)}
    }};
}};
            """
        return code

    def generate_enum(self, name, enumerants):
        sep = ',\n' + 4 * ' '
        code = \
            f"""
enum class {name} {{
    {sep.join(enumerants)},
}};
            """
        return code

    def generate(self, nodes):
        res = ""
        for node, content in nodes.items():
            if type(content) is list:
                res += self.generate_enum(node, content)
            else:
                res += self.generate_struct(node, content)

        return res


def main():
    p = Parser()
    nodes = p.parse(SPEC_FILE if len(sys.argv) < 2 else sys.argv[1])

    g = Generator(nodes)
    text = g.generate(nodes)
    print(text)

    # try:
    #     os.remove(GENERATED_STRUCTS_FILE)
    # except:
    #     pass
    #
    # for item in ast.items:
    #     struct: str = item.process()
    #     with open(GENERATED_STRUCTS_FILE, mode='a') as res:
    #         res.write(struct)
    #
    # return 0

if __name__ == '__main__':
    main()
