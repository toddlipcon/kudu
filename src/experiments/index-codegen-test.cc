// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#define __STDC_LIMIT_MACROS
#define __STDC_FORMAT_MACROS
#define __STDC_CONSTANT_MACROS
#include <limits.h>
#include <stdio.h>

#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/scoped_ptr.hpp>
#include <glog/logging.h>
#include <llvm/LLVMContext.h>
#include <llvm/Module.h>
#include <llvm/Constants.h>
#include <llvm/Function.h>
#include <llvm/BasicBlock.h>
#include <llvm/PassManager.h>

#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/IRBuilder.h>
#include <llvm/Pass.h>
#include <llvm/PassManager.h>
#include <llvm/Analysis/Verifier.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JIT.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include "llvm/Transforms/IPO.h"

#include "util/stopwatch.h"


using namespace llvm;
using boost::scoped_ptr;
using std::string;
using std::vector;
using kudu::LogTiming;

Module* makeLLVMModule(LLVMContext &ctx);

class BinarySearchCompiler : boost::noncopyable {
public:
  typedef int (*binary_search_func_t)(const char *s, size_t len);

  BinarySearchCompiler(LLVMContext &ctx, const std::vector<string> &entries) :
    ctx_(ctx),
    size_t_type_(IntegerType::get(ctx, sizeof(size_t) * 8)),
    int_type_(IntegerType::get(ctx, sizeof(int) * 8)),
    char_p_type_(Type::getInt8PtrTy(ctx)),
    entries_(entries)
  {
    mod_ = new Module("my_module", ctx_);
    f_memcmp_ = CreateMyMemcmp(mod_);
  }

  void Compile() {
    Function *func = CreateBinarySearchFunction();

    BasicBlock *entry = BasicBlock::Create(ctx_, "entry", func);
    BasicBlock *bin_search = GenerateBinarySearch(0, entries_.size() - 1, func);
    IRBuilder<> builder(entry);
    builder.CreateBr(bin_search);

  }

  Module *module() {
    return CHECK_NOTNULL(mod_);
  }

private:

  Function *CreateBinarySearchFunction() {
    Function *func = reinterpret_cast<Function *>(
      mod_->getOrInsertFunction("binary_search",
                                int_type_,
                                char_p_type_,
                                size_t_type_,
                                NULL));
    CHECK(func);
    func->setCallingConv(CallingConv::C);

    // Setup args
    Function::arg_iterator args = func->arg_begin();
    s_arg_ = args++; s_arg_->setName("s");
    len_arg_ = args++; len_arg_->setName("len");
    return func;
  }

  BasicBlock *GenerateBinarySearch(ssize_t left, ssize_t right, Function *func) {
    if (left <= right) {
      BasicBlock *compare_block = CHECK_NOTNULL(BasicBlock::Create(ctx_, "compare", func));

      ssize_t mid = (left + right + 1) / 2;
      ArrayRef<uint8_t> mid_entry(
        reinterpret_cast<const uint8_t *>(entries_[mid].c_str()),
        entries_[mid].size());
      Constant *mid_key = CHECK_NOTNULL(ConstantDataArray::get(ctx_, mid_entry));
      Constant *mid_entry_len = ConstantInt::get(Type::getInt32Ty(ctx_), entries_[mid].size());

      IRBuilder<> builder(compare_block);

      GlobalVariable *gv = new GlobalVariable(*mod_, mid_key->getType(), true,
                                              GlobalValue::PrivateLinkage, mid_key,
                                              "gv");
      Value *mid_key_ptr = builder.CreateConstInBoundsGEP2_32(gv, 0, 0, "myptr");
      Value *memcmp_ret = builder.CreateCall3(f_memcmp_, s_arg_, mid_key_ptr, mid_entry_len,
                                              "call_memcmp");
      Value *lessthan = builder.CreateICmpSLT(memcmp_ret, builder.getInt32(0), "cmp");

      BasicBlock *lt_branch = GenerateBinarySearch(left, mid - 1, func);
      BasicBlock *geq_branch = GenerateBinarySearch(mid + 1, right, func);
      builder.CreateCondBr(lessthan, lt_branch, geq_branch);
      return compare_block;
    } else {
      BasicBlock *ret_block = BasicBlock::Create(ctx_, "ret", func);
      IRBuilder<> builder(ret_block);
      builder.CreateRet(ConstantInt::get(int_type_, left));
      return ret_block;
    }
  }

  Function *LookupStrlen(Module *mod) {
    std::vector<Type *>strlen_args;
    strlen_args.push_back(char_p_type_);
    FunctionType *strlen_type = FunctionType::get(size_t_type_, strlen_args, false);

    Function *f_strlen = Function::Create(
      strlen_type, GlobalValue::ExternalLinkage, "strlen", mod);
    return CHECK_NOTNULL(f_strlen);
  }

  Function *LookupMemcmp(Module *mod) {
    std::vector<Type *>memcmp_args;
    memcmp_args.push_back(char_p_type_);
    memcmp_args.push_back(char_p_type_);
    memcmp_args.push_back(IntegerType::get(ctx_, 32));
    FunctionType *memcmp_type = FunctionType::get(int_type_, memcmp_args, false);
    Function *f_memcmp = Function::Create(
      memcmp_type, GlobalValue::ExternalLinkage, "memcmp", mod);
    return CHECK_NOTNULL(f_memcmp);
  }

  Function *CreateMyMemcmp(Module *mod) {
    // Type Definitions
    std::vector<Type*>FuncTy_0_args;
    PointerType* PointerTy_1 = PointerType::get(IntegerType::get(mod->getContext(), 8), 0);
 
    FuncTy_0_args.push_back(PointerTy_1);
    FuncTy_0_args.push_back(PointerTy_1);
    FuncTy_0_args.push_back(IntegerType::get(mod->getContext(), 32));
    FunctionType* FuncTy_0 = FunctionType::get(
      /*Result=*/IntegerType::get(mod->getContext(), 32),
      /*Params=*/FuncTy_0_args,
      /*isVarArg=*/false);
 

    Function* func_my_memcmp = mod->getFunction("my_memcmp");
    if (!func_my_memcmp) {
      func_my_memcmp = Function::Create(
        /*Type=*/FuncTy_0,
        /*Linkage=*/GlobalValue::ExternalLinkage,
        /*Name=*/"my_memcmp", mod); 
      func_my_memcmp->setCallingConv(CallingConv::C);
    }
    AttrListPtr func_my_memcmp_PAL;
    {
      SmallVector<AttributeWithIndex, 4> Attrs;
      AttributeWithIndex PAWI;
      PAWI.Index = 1U;
      {
        AttrBuilder B;
        B.addAttribute(Attributes::NoCapture);
        PAWI.Attrs = Attributes::get(mod->getContext(), B);
      }
      Attrs.push_back(PAWI);
      PAWI.Index = 2U;
      {
        AttrBuilder B;
        B.addAttribute(Attributes::NoCapture);
        PAWI.Attrs = Attributes::get(mod->getContext(), B);
      }
      Attrs.push_back(PAWI);
      PAWI.Index = 4294967295U;
      {
        AttrBuilder B;
        B.addAttribute(Attributes::NoUnwind);
        B.addAttribute(Attributes::ReadOnly);
        B.addAttribute(Attributes::UWTable);
        PAWI.Attrs = Attributes::get(mod->getContext(), B);
      }
      Attrs.push_back(PAWI);
      func_my_memcmp_PAL = AttrListPtr::get(mod->getContext(), Attrs);
  
    }
    func_my_memcmp->setAttributes(func_my_memcmp_PAL);
 
    // Global Variable Declarations

 
    // Constant Definitions
    ConstantInt* const_int32_2 = ConstantInt::get(mod->getContext(), APInt(32, StringRef("0"), 10));
    ConstantInt* const_int32_3 = ConstantInt::get(mod->getContext(), APInt(32, StringRef("-1"), 10));
    ConstantInt* const_int64_4 = ConstantInt::get(mod->getContext(), APInt(64, StringRef("1"), 10));
 
    // Global Variable Definitions
 
    // Function Definitions
 
    // Function: my_memcmp (func_my_memcmp)
    {
      Function::arg_iterator args = func_my_memcmp->arg_begin();
      Value* ptr_buf1 = args++;
      ptr_buf1->setName("buf1");
      Value* ptr_buf2 = args++;
      ptr_buf2->setName("buf2");
      Value* int32_count = args++;
      int32_count->setName("count");
  
      BasicBlock* label_5 = BasicBlock::Create(mod->getContext(), "",func_my_memcmp,0);
      BasicBlock* label__preheader = BasicBlock::Create(mod->getContext(), ".preheader",func_my_memcmp,0);
      BasicBlock* label__lr_ph = BasicBlock::Create(mod->getContext(), ".lr.ph",func_my_memcmp,0);
      BasicBlock* label_6 = BasicBlock::Create(mod->getContext(), "",func_my_memcmp,0);
      BasicBlock* label__critedge = BasicBlock::Create(mod->getContext(), ".critedge",func_my_memcmp,0);
      BasicBlock* label_7 = BasicBlock::Create(mod->getContext(), "",func_my_memcmp,0);
  
      // Block  (label_5)
      ICmpInst* int1_8 = new ICmpInst(*label_5, ICmpInst::ICMP_EQ, int32_count, const_int32_2, "");
      BranchInst::Create(label_7, label__preheader, int1_8, label_5);
  
      // Block .preheader (label__preheader)
      BinaryOperator* int32_10 = BinaryOperator::Create(Instruction::Add, int32_count, const_int32_3, "", label__preheader);
      ICmpInst* int1_11 = new ICmpInst(*label__preheader, ICmpInst::ICMP_EQ, int32_10, const_int32_2, "");
      BranchInst::Create(label__critedge, label__lr_ph, int1_11, label__preheader);
  
      // Block .lr.ph (label__lr_ph)
      Argument* fwdref_14 = new Argument(IntegerType::get(mod->getContext(), 32));
      PHINode* int32_13 = PHINode::Create(IntegerType::get(mod->getContext(), 32), 2, "", label__lr_ph);
      int32_13->addIncoming(fwdref_14, label_6);
      int32_13->addIncoming(int32_10, label__preheader);
  
      Argument* fwdref_15 = new Argument(PointerTy_1);
      PHINode* ptr__0812 = PHINode::Create(PointerTy_1, 2, ".0812", label__lr_ph);
      ptr__0812->addIncoming(fwdref_15, label_6);
      ptr__0812->addIncoming(ptr_buf2, label__preheader);
  
      Argument* fwdref_16 = new Argument(PointerTy_1);
      PHINode* ptr__0911 = PHINode::Create(PointerTy_1, 2, ".0911", label__lr_ph);
      ptr__0911->addIncoming(fwdref_16, label_6);
      ptr__0911->addIncoming(ptr_buf1, label__preheader);
  
      LoadInst* int8_17 = new LoadInst(ptr__0911, "", false, label__lr_ph);
      int8_17->setAlignment(1);
      LoadInst* int8_18 = new LoadInst(ptr__0812, "", false, label__lr_ph);
      int8_18->setAlignment(1);
      ICmpInst* int1_19 = new ICmpInst(*label__lr_ph, ICmpInst::ICMP_EQ, int8_17, int8_18, "");
      BranchInst::Create(label_6, label__critedge, int1_19, label__lr_ph);
  
      // Block  (label_6)
      GetElementPtrInst* ptr_21 = GetElementPtrInst::Create(ptr__0911, const_int64_4, "", label_6);
      GetElementPtrInst* ptr_22 = GetElementPtrInst::Create(ptr__0812, const_int64_4, "", label_6);
      BinaryOperator* int32_23 = BinaryOperator::Create(Instruction::Add, int32_13, const_int32_3, "", label_6);
      ICmpInst* int1_24 = new ICmpInst(*label_6, ICmpInst::ICMP_EQ, int32_23, const_int32_2, "");
      BranchInst::Create(label__critedge, label__lr_ph, int1_24, label_6);
  
      // Block .critedge (label__critedge)
      PHINode* ptr__08_lcssa = PHINode::Create(PointerTy_1, 3, ".08.lcssa", label__critedge);
      ptr__08_lcssa->addIncoming(ptr_buf2, label__preheader);
      ptr__08_lcssa->addIncoming(ptr__0812, label__lr_ph);
      ptr__08_lcssa->addIncoming(ptr_22, label_6);
  
      PHINode* ptr__09_lcssa = PHINode::Create(PointerTy_1, 3, ".09.lcssa", label__critedge);
      ptr__09_lcssa->addIncoming(ptr_buf1, label__preheader);
      ptr__09_lcssa->addIncoming(ptr__0911, label__lr_ph);
      ptr__09_lcssa->addIncoming(ptr_21, label_6);
  
      LoadInst* int8_26 = new LoadInst(ptr__09_lcssa, "", false, label__critedge);
      int8_26->setAlignment(1);
      CastInst* int32_27 = new ZExtInst(int8_26, IntegerType::get(mod->getContext(), 32), "", label__critedge);
      LoadInst* int8_28 = new LoadInst(ptr__08_lcssa, "", false, label__critedge);
      int8_28->setAlignment(1);
      CastInst* int32_29 = new ZExtInst(int8_28, IntegerType::get(mod->getContext(), 32), "", label__critedge);
      BinaryOperator* int32_30 = BinaryOperator::Create(Instruction::Sub, int32_27, int32_29, "", label__critedge);
      BranchInst::Create(label_7, label__critedge);
  
      // Block  (label_7)
      PHINode* int32__010 = PHINode::Create(IntegerType::get(mod->getContext(), 32), 2, ".010", label_7);
      int32__010->addIncoming(int32_30, label__critedge);
      int32__010->addIncoming(const_int32_2, label_5);
  
      ReturnInst::Create(mod->getContext(), int32__010, label_7);
  
      // Resolve Forward References
      fwdref_16->replaceAllUsesWith(ptr_21); delete fwdref_16;
      fwdref_15->replaceAllUsesWith(ptr_22); delete fwdref_15;
      fwdref_14->replaceAllUsesWith(int32_23); delete fwdref_14;
  
    }
    return func_my_memcmp;
  } 



  LLVMContext &ctx_;

  Type *size_t_type_;
  Type *int_type_;
  Type *char_p_type_;

  std::vector<string> entries_;

  Module* mod_;
  Function *f_memcmp_;

  // Function in-progress
  Value *s_arg_;
  Value *len_arg_;
};

void OptimizeModule(Module *m) {
  PassManagerBuilder pmb;
  pmb.OptLevel = 3;
  pmb.Inliner = createFunctionInliningPass();

  scoped_ptr<FunctionPassManager> function_pass(new FunctionPassManager(m));
  pmb.populateFunctionPassManager(*function_pass);
  for (Module::iterator it = m->begin(), end = m->end(); it != end ; ++ it) {
    if (!it->isDeclaration()) function_pass->run(*it);
  }
  function_pass->doFinalization() ;

  scoped_ptr<PassManager> pm(new PassManager());
  pmb.populateModulePassManager(*pm);
  pm->run(*m);
}

static int DoCheck(BinarySearchCompiler::binary_search_func_t fp, const uint8_t *s, int size) {
  return fp((const char *)s, size);
}

int main(int argc, char**argv) {
  InitializeNativeTarget();

  vector<string> keys;
  for (int i = 0; i < 10; i++) {
    uint8_t buf[100];
    for (int j = 0; j < 8; j++) {
      buf[j] = (uint8_t)random();
    }
    keys.push_back(string(reinterpret_cast<char *>(buf), 8));
  }
  std::sort(keys.begin(), keys.end());

  BinarySearchCompiler comp(getGlobalContext(), keys);
  comp.Compile();

  Module *mod = comp.module();
  //mod->dump();
  verifyModule(*mod, PrintMessageAction);

  LOG(INFO) << "Optimizing...";
  OptimizeModule(mod);
  mod->dump();

  // Create the JIT.  This takes ownership of the module.
  std::string errStr;
  ExecutionEngine *engine = EngineBuilder(mod).setErrorStr(&errStr).create();
  CHECK(engine) << "Cannot create engine: " << errStr;


  Function *f = CHECK_NOTNULL(mod->getFunction("binary_search"));
  BinarySearchCompiler::binary_search_func_t fp = reinterpret_cast<BinarySearchCompiler::binary_search_func_t>(
    engine->getPointerToFunction(f));

  LOG_TIMING(INFO, "100M lookups") {
    for (int i = 0; i < 1000*1000*100; i++) {
      uint8_t buf[100];
      for (int j = 0; j < 8; j++) {
        buf[j] = (uint8_t)random();
      }
      DoCheck(fp, buf, 8);
    }
  }
  return 0;
}

