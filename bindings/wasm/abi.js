async function run() {
    var stack = [];
    var ref_counted = {};
    
    // id >= 0 for values on stack
    // id < 0 for values in ref_counted
    
    function stackPush(value) {
        stack.push(value);
        return stack.length - 1;
    }
    
    function pushU32(int) {
        return stackPush(int);
    }
    
    function pushString(address, length) {
        let bytes = new Uint8Array(dida.instance.exports.memory.buffer);
        let string = new TextDecoder().decode(bytes.slice(address, address + length));
        return stackPush(string);
    }
    
    function pushObject() {
        return stackPush({});
    }
    
    function setProperty(object_id, name_id, value_id) {
        stack[object_id][stack[name_id]] = stack[value_id];
    }
    
    function consoleLog(message_id) {
        console.log(stack[message_id]);
    }
    
    dida = await WebAssembly.instantiateStreaming(
        fetch("./dida.wasm"),
        {
            env: {
                pushU32: pushU32,
                pushString: pushString,
                pushObject: pushObject,
                setProperty: setProperty,
                consoleLog: consoleLog,
            },
        }
    );
    
    // TODO codegen this
    function GraphBuilder() {
        const init_stack_len = stack.length;
        const result_ix = dida.instance.exports.GraphBuilder_init();
        const result = stack[result_ix];
        stack.length = init_stack_len;
        this.external = result.external;
    }
    
    console.log(new GraphBuilder());
}