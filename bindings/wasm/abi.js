async function Abi(dida_url) {
    var stack = [];
    var ref_counted = {};
    
    // id >= 0 for values on stack
    // id < 0 for values in ref_counted
    
    function stackGetLength() {
        return stack.length;
    }
    
    function stackReset(length) {
        stack.length = length;
    }
    
    function stackRead(ix) {
        return stack[ix];
    }

    function stackPush(value) {
        stack.push(value);
        return stack.length - 1;
    }
    
    function pushString(address, length) {
        let bytes = new Uint8Array(wasm.instance.exports.memory.buffer);
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
    
    const wasm = await WebAssembly.instantiateStreaming(
        fetch(dida_url),
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
            }
        }
    );
    
    return {
        wasm: wasm,
        stackGetLength: stackGetLength,
        stackReset: stackReset,
        stackRead: stackRead,
        stackPush: stackPush,
    };
}