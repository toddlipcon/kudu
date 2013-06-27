// Copyright (c) 2013, Cloudera, inc.

#include "util/status.h"

#include <boost/foreach.hpp>
#include <ctype.h>
#include <glog/logging.h>
#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/stubs/common.h>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

#include <gutil/gscoped_ptr.h>
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "util/string_case.h"

using google::protobuf::FileDescriptor;
using google::protobuf::MethodDescriptor;
using google::protobuf::ServiceDescriptor;
using google::protobuf::io::Printer;
using std::map;
using std::string;
using std::vector;

namespace kudu {
namespace rpc {

class Substituter {
 public:
  virtual ~Substituter() {}
  virtual void InitSubstitutionMap(map<string, string> *map) const = 0;
};

// NameInfo contains information about the output names.
class FileSubstitutions : public Substituter {
 public:
  static const std::string PROTO_EXTENSION;

  Status Init(const FileDescriptor *file) {
    string path = file->name();
    map_["path"] = path;

    // Initialize path_
    // If path = /foo/bar/baz_stuff.proto, path_ = /foo/bar/baz_stuff
    if (!TryStripSuffixString(path, PROTO_EXTENSION, &path_no_extension_)) {
      return Status::InvalidArgument("file name " + path +
                                     " did not end in " + PROTO_EXTENSION);
    }

    // If path = /foo/bar/baz_stuff.proto, base_ = baz_stuff
    string base;
    GetBaseName(path_no_extension_, &base);
    map_["base"] = base;

    // If path = /foo/bar/baz_stuff.proto, camel_case_ = BazStuff
    string camel_case;
    SnakeToCamelCase(base, &camel_case);
    map_["camel_case"] = camel_case;

    // If path = /foo/bar/baz_stuff.proto, upper_case_ = BAZ_STUFF
    string upper_case;
    ToUpperCase(base, &upper_case);
    map_["upper_case"] = upper_case;

    map_["open_namespace"] = GenerateOpenNamespace(file->package());
    map_["close_namespace"] = GenerateCloseNamespace(file->package());

    return Status::OK();
  }

  virtual void InitSubstitutionMap(map<string, string> *map) const {
    typedef std::map<string, string>::value_type kv_pair;
    BOOST_FOREACH(const kv_pair &pair, map_) {
      (*map)[pair.first] = pair.second;
    }
  }

  std::string service_header() const {
    return path_no_extension_ + ".service.h";
  }

  std::string service() const {
    return path_no_extension_ + ".service.cc";
  }

  std::string proxy_header() const {
    return path_no_extension_ + ".proxy.h";
  }

  std::string proxy() const {
    return path_no_extension_ + ".proxy.cc";
  }

 private:
  // Extract the last filename component.
  static void GetBaseName(const string &path,
                          string *base) {
    size_t last_slash = path.find_last_of("/");
    if (last_slash != string::npos) {
      *base = path.substr(last_slash + 1);
    } else {
      *base = path;
    }
  }

  static string GenerateOpenNamespace(const string &str) {
    vector<string> components = strings::Split(str, ".");
    string out;
    BOOST_FOREACH(const string &c, components) {
      out.append("namespace ").append(c).append(" {\n");
    }
    return out;
  }

  static string GenerateCloseNamespace(const string &str) {
    vector<string> components = strings::Split(str, ".");
    string out;
    BOOST_FOREACH(const string &c, components) {
      out.append("} // namespace ").append(c).append("\n");
    }
    return out;
  }

  std::string path_no_extension_;
  map<string, string> map_;
};

const std::string FileSubstitutions::PROTO_EXTENSION(".proto");

class MethodSubstitutions : public Substituter {
 public:
  MethodSubstitutions(const MethodDescriptor *method)
    : rpc_name_(method->name()),
      request_(method->input_type()->name()),
      response_(method->output_type()->name())
  {
  }

  virtual void InitSubstitutionMap(map<string, string> *map) const {
    (*map)["rpc_name"] = rpc_name_;
    (*map)["request"] = request_;
    (*map)["response"] = response_;
  }

 private:
  std::string rpc_name_;
  std::string request_;
  std::string response_;
};

class ServiceSubstitutions : public Substituter {
 public:
  ServiceSubstitutions(const ServiceDescriptor *service)
    : service_(service)
  {}

  virtual void InitSubstitutionMap(map<string, string> *map) const {
    (*map)["service_name"] = service_->name();
    // TODO: upgrade to protobuf 2.5.x and attach service comments
    // to the generated service classes using the SourceLocation API.
  }

 private:
  const ServiceDescriptor *service_;
};


class SubstitutionContext {
 public:
  ~SubstitutionContext() {
    while (!subs_.empty()) {
      Pop();
    }
  }

  // Takes ownership of the substituter
  void Push(const Substituter *sub) {
    subs_.push_back(sub);
  }

  void PushMethod(const MethodDescriptor *method) {
    subs_.push_back(new MethodSubstitutions(method));
  }

  void PushService(const ServiceDescriptor *service) {
    subs_.push_back(new ServiceSubstitutions(service));
  }

  void Pop() {
    CHECK(!subs_.empty());
    delete subs_.back();
    subs_.pop_back();
  }

  void InitSubstitutionMap(map<string, string> *subs) const {
    BOOST_FOREACH(const Substituter *sub, subs_) {
      sub->InitSubstitutionMap(subs);
    }
  }

 private:
  vector<const Substituter*> subs_;
};



class CodeGenerator : public ::google::protobuf::compiler::CodeGenerator {
 public:
  CodeGenerator() { }

  ~CodeGenerator() { }

  bool Generate(const google::protobuf::FileDescriptor *file,
        const std::string &/* parameter */,
        google::protobuf::compiler::GeneratorContext *gen_context,
        std::string *error) const {
    FileSubstitutions *name_info = new FileSubstitutions();
    Status ret = name_info->Init(file);
    if (!ret.ok()) {
      *error = "name_info.Init failed: " + ret.ToString();
      return false;
    }

    SubstitutionContext subs;
    subs.Push(name_info);

    gscoped_ptr<google::protobuf::io::ZeroCopyOutputStream> ih_output(
        gen_context->Open(name_info->service_header()));
    Printer ih_printer(ih_output.get(), '$');
    GenerateServiceIfHeader(&ih_printer, &subs, file);

    gscoped_ptr<google::protobuf::io::ZeroCopyOutputStream> i_output(
        gen_context->Open(name_info->service()));
    Printer i_printer(i_output.get(), '$');
    GenerateServiceIf(&i_printer, &subs, file);

    gscoped_ptr<google::protobuf::io::ZeroCopyOutputStream> ph_output(
        gen_context->Open(name_info->proxy_header()));
    Printer ph_printer(ph_output.get(), '$');
    GenerateProxyHeader(&ph_printer, &subs, file);

    gscoped_ptr<google::protobuf::io::ZeroCopyOutputStream> p_output(
        gen_context->Open(name_info->proxy()));
    Printer p_printer(p_output.get(), '$');
    GenerateProxy(&p_printer, &subs, file);

    return true;
  }

 private:
  void Print(Printer *printer,
             const SubstitutionContext &sub,
             const char *text) const {
    map<string, string> subs;
    sub.InitSubstitutionMap(&subs);
    printer->Print(subs, text);
  }

  void GenerateServiceIfHeader(Printer *printer,
                               SubstitutionContext *subs,
                               const FileDescriptor *file) const {
    Print(printer, *subs,
      "// Copyright (c) 2013, Cloudera, inc.\n"
      "// THIS FILE IS AUTOGENERATED FROM $path$.proto\n"
      "\n"
      "#ifndef KUDU_RPC_$upper_case$_SERVICE_IF_DOT_H\n"
      "#define KUDU_RPC_$upper_case$_SERVICE_IF_DOT_H\n"
      "\n"
      "#include \"$base$.pb.h\"\n"
      "\n"
      "#include \"rpc/rpc_header.pb.h\"\n"
      "#include \"rpc/service_if.h\"\n"
      "\n"
      "namespace kudu { namespace rpc { class Messenger; } }\n"
      "$open_namespace$"
      "\n"
      );

    for (int service_idx = 0; service_idx < file->service_count();
         ++service_idx) {
      const ServiceDescriptor *service = file->service(service_idx);
      subs->PushService(service);

      // TODO: it seems like this is generating FooServiceIf from foo.proto even if
      // the underlying pb "service" definition has some other name. That's wrong.
      Print(printer, *subs,
        "\n"
        "class $service_name$If : public ::kudu::rpc::ServiceIf {\n"
        "public:\n"
        "  virtual ~$service_name$If();\n"
        "  virtual void Handle(::kudu::rpc::InboundCall *call);\n"
        );

      for (int method_idx = 0; method_idx < service->method_count();
           ++method_idx) {
        const MethodDescriptor *method = service->method(method_idx);
        subs->PushMethod(method);

        Print(printer, *subs,
        "  virtual void $rpc_name$(const $request$ *req,\n"
        "     $response$ *resp, ::kudu::rpc::RpcContext *context) = 0;\n"
            );

        subs->Pop();
      }
      Print(printer, *subs,
        "};\n");
      subs->Pop();
    }

    Print(printer, *subs,
      "\n"
      "$close_namespace$\n"
      "#endif\n");
  }

  void GenerateServiceIf(Printer *printer,
                         SubstitutionContext *subs,
                         const FileDescriptor *file) const {
    Print(printer, *subs,
      "// Copyright (c) 2013, Cloudera, inc.\n"
      "// THIS FILE IS AUTOGENERATED FROM $path$.proto\n"
      "\n"
      "#include \"$base$.pb.h\"\n"
      "#include \"$base$.service.h\"\n"
      "\n"
      "#include <glog/logging.h>\n"
      "\n"
      "#include \"rpc/server_call.h\"\n"
      "\n"
      "$open_namespace$"
      "\n");

    for (int service_idx = 0; service_idx < file->service_count();
         ++service_idx) {
      const ServiceDescriptor *service = file->service(service_idx);
      subs->PushService(service);

      Print(printer, *subs,
        "$service_name$If::~$service_name$If() {\n"
        "}\n"
        "\n"
        "void $service_name$If::Handle(::kudu::rpc::InboundCall *call) {\n"
        "  {\n");

      for (int method_idx = 0; method_idx < service->method_count();
           ++method_idx) {
        const MethodDescriptor *method = service->method(method_idx);
        subs->PushMethod(method);

        Print(printer, *subs,
        "    if (call->method_name() == \"$rpc_name$\") {\n"
        "      $request$ *req = new $request$;\n"
        "      if (PREDICT_FALSE(!ParseParam(call, req))) {\n"
        "        delete req;\n"
        "        return;\n"
        "      }\n"
        "      $response$ *resp = new $response$;\n"
        "      $rpc_name$(req, resp, new ::kudu::rpc::RpcContext(call, req, resp));\n"
        "      return;\n"
        "    }\n"
        "\n");
        subs->Pop();
      }
      Print(printer, *subs,
        "  }\n"
        "  RespondBadMethod(call);\n"
        "}\n");
      subs->Pop();
    }
    Print(printer, *subs,
      "\n"
      "$close_namespace$"
      );
  }

  void GenerateProxyHeader(Printer *printer,
                           SubstitutionContext *subs,
                           const FileDescriptor *file) const {
    Print(printer, *subs,
      "// Copyright (c) 2013, Cloudera, inc.\n"
      "// THIS FILE IS AUTOGENERATED FROM $path$.proto\n"
      "\n"
      "#ifndef KUDU_RPC_$upper_case$_PROXY_DOT_H\n"
      "#define KUDU_RPC_$upper_case$_PROXY_DOT_H\n"
      "\n"
      "#include \"$base$.pb.h\""
      "\n"
      "#include \"rpc/proxy.h\"\n"
      "#include \"util/status.h\"\n"
      "\n"
      "namespace kudu { namespace rpc { class Sockaddr; } }\n"
      "$open_namespace$"
      "\n"
      "\n"
    );

    for (int service_idx = 0; service_idx < file->service_count();
         ++service_idx) {
      const ServiceDescriptor *service = file->service(service_idx);
      subs->PushService(service);

      Print(printer, *subs,
        "class $service_name$Proxy {\n"
        " public:\n"
        "  $service_name$Proxy(const std::tr1::shared_ptr< ::kudu::rpc::Messenger>\n"
        "                &messenger, const ::kudu::rpc::Sockaddr &sockaddr);\n"
        "  ~$service_name$Proxy();\n"
        );

      for (int method_idx = 0; method_idx < service->method_count();
           ++method_idx) {
        const MethodDescriptor *method = service->method(method_idx);
        subs->PushMethod(method);

        Print(printer, *subs,
        "\n"
        "  ::kudu::Status $rpc_name$(const $request$ &req, $response$ *resp,\n"
        "                          ::kudu::rpc::RpcController *controller);\n"
        "  void $rpc_name$Async(const $request$ &req,\n"
        "                       $response$ *response,\n"
        "                       ::kudu::rpc::RpcController *controller,\n"
        "                       const ::kudu::rpc::ResponseCallback &callback);\n"
        );
        subs->Pop();
      }
      Print(printer, *subs,
      " private:\n"
      "  ::kudu::rpc::Proxy proxy_;\n"
      "};\n");
      subs->Pop();
    }
    Print(printer, *subs,
      "\n"
      "$close_namespace$"
      "\n"
      "#endif\n"
      );
  }

  void GenerateProxy(Printer *printer,
                     SubstitutionContext *subs,
                     const FileDescriptor *file) const {
    Print(printer, *subs,
      "// Copyright (c) 2013, Cloudera, inc.\n"
      "// THIS FILE IS AUTOGENERATED FROM $path$.proto\n"
      "\n"
      "#include \"$base$.proxy.h\"\n"
      "\n"
      "#include \"rpc/messenger.h\"\n"
      "#include \"rpc/sockaddr.h\"\n"
      "\n"
      "$open_namespace$"
      "\n"
      );

    for (int service_idx = 0; service_idx < file->service_count();
         ++service_idx) {
      const ServiceDescriptor *service = file->service(service_idx);
      subs->PushService(service);
      Print(printer, *subs,
        "$service_name$Proxy::$service_name$Proxy(const std::tr1::shared_ptr< ::kudu::rpc::Messenger> &messenger,\n"
        "                                   const ::kudu::rpc::Sockaddr &remote)\n"
        "  : proxy_(messenger, remote)\n"
        "{\n"
        "}\n"
        "\n"
        "$service_name$Proxy::~$service_name$Proxy() {\n"
        "}\n"
        "\n");
      for (int method_idx = 0; method_idx < service->method_count();
           ++method_idx) {
        const MethodDescriptor *method = service->method(method_idx);
        subs->PushMethod(method);
        Print(printer, *subs,
        "Status $service_name$Proxy::$rpc_name$(const $request$ &req, $response$ *resp,\n"
        "                                     ::kudu::rpc::RpcController *controller) {\n"
        "  return proxy_.SyncRequest(\"$rpc_name$\", req, resp, controller);\n"
        "}\n"
        "\n"
        "void $service_name$Proxy::$rpc_name$Async(const $request$ &req,\n"
        "                     $response$ *resp, ::kudu::rpc::RpcController *controller,\n"
        "                     const ::kudu::rpc::ResponseCallback &callback) {\n"
        "  proxy_.AsyncRequest(\"$rpc_name$\", req, resp, controller, callback);\n"
        "}\n"
        "\n");
        subs->Pop();
      }

      subs->Pop();
    }
    Print(printer, *subs,
      "$close_namespace$");
  }
};
} // namespace rpc
} // namespace kudu

int main(int argc, char *argv[]) {
  kudu::rpc::CodeGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
