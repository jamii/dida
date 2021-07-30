function Debugger(abi) {

function frame() {
    const init_stack_len = abi.stackGetLength();
    const result_ix = abi.wasm.instance.exports.frame();
    const result = abi.stackRead(result_ix);
    abi.stackReset(init_stack_len);
    return result;
}

this.frame = frame;

}