async function run() {
    var stack = [];
    var ref_counted = {};
    
    // id >= 0 for values on stack
    // id < 0 for values in ref_counted
    
    function stackRead(ix) {
        return stack[ix];
    }

    function stackPush(value) {
        stack.push(value);
        return stack.length - 1;
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
    
    function getProperty(object_id, name_id) {
        return stackPush(stack[object_id][stack[name_id]]);
    }

    function consoleLog(message_id) {
        console.log(stack[message_id]);
    }
    
    function consoleError(message_id) {
        console.error(stack[message_id]);
    }
    
    dida = await WebAssembly.instantiateStreaming(
        fetch("./dida.wasm"),
        {
            env: {
                getU32: stackRead,
                getI32: stackRead,
                pushU32: stackPush,
                pushI32: stackPush,
                pushString: pushString,
                pushObject: pushObject,
                getProperty: getProperty,
                setProperty: setProperty,
                consoleLog: consoleLog,
                consoleError: consoleError,
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
    
    GraphBuilder.prototype.addSubgraph = function addSubgraph(parent) {
        const init_stack_len = stack.length;
        const result_ix = dida.instance.exports.GraphBuilder_addSubgraph(
            stackPush(this),
            stackPush(parent),
        );
        const result = stack[result_ix];
        stack.length = init_stack_len;
        return result;
    };
    
    // TODO just grab core.js?
    const g = new GraphBuilder();
    console.log(g);
    console.log(g.addSubgraph({id: 0}));
}