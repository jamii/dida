export default async function Abi(dida_wasm_bytes) {
  // id >= 0 for values on stack
  // id < 0 for values in ref_counted

  var stack = [];
  var ref_counted = {};
  var next_ref_counted_id = -1;

  function stackPush(value) {
    stack.push(value);
    return stack.length - 1;
  }

  function stackRead(ix) {
    return stack[ix];
  }

  function stackGetLength() {
    return stack.length;
  }

  function stackReset(length) {
    stack.length = length;
  }

  function RefCounted(value, refcount) {
    this.value = value;
    this.refcount = refcount;
  }

  function createRefCounted(value_id, refcount) {
    const ref_counted_id = next_ref_counted_id;
    next_ref_counted_id -= 1;
    ref_counted[ref_counted_id] = new RefCounted(stackRead(value_id), refcount);
    return ref_counted_id
  }

  function getRefCounted(ref_counted_id) {
    return stackPush(ref_counted[ref_counted_id].value);
  }

  // Return values must be kept in sync with js_common.JsType
  function jsTypeOf(value_ix) {
    const value = stackRead(value_ix);
    const typ = typeof (value);
    if (typ == 'undefined') return 0;
    // typeof(null) == 'object' :|
    if (value == null) return 1;
    if (typ == 'boolean') return 2;
    if (typ == 'number') return 3;
    if (typ == 'string') return 4;
    if (typ == 'object') return 5;
    if (typ == 'function') return 6;
    throw (typ + ' is not a type that the abi understands');
  }

  function createUndefined() {
    return stackPush(undefined);
  }

  function createString(address, length) {
    let bytes = new Uint8Array(wasm.instance.exports.memory.buffer);
    let string = new TextDecoder().decode(bytes.slice(address, address + length));
    return stackPush(string);
  }

  function createObject() {
    return stackPush({});
  }

  function createArray(len) {
    return stackPush(new Array(len));
  }

  function getStringLength(string_id) {
    return stackRead(string_id).length;
  }

  function getStringInto(string_id, address, max_len) {
    const string = stackRead(string_id);
    const encoded = new TextEncoder().encode(string);
    const bytes = new Uint8Array(wasm.instance.exports.memory.buffer);
    const len = Math.min(string.length, max_len);
    bytes.set(encoded.subarray(0, len), address);
    return len;
  }

  function getArrayLength(array_id) {
    return stackRead(array_id).length;
  }

  function getElement(array_id, ix) {
    return stackPush(stackRead(array_id)[ix]);
  }

  function setElement(array_id, ix, value_id) {
    stackRead(array_id)[ix] = stackRead(value_id);
  }

  function getProperty(object_id, name_id) {
    return stackPush(stackRead(object_id)[stackRead(name_id)]);
  }

  function setProperty(object_id, name_id, value_id) {
    stackRead(object_id)[stackRead(name_id)] = stackRead(value_id);
  }

  function callFunction(function_id, args_id) {
    return stackPush(stackRead(function_id).apply(null, stackRead(args_id)));
  }

  function consoleLog(message_id) {
    console.log(stackRead(message_id));
  }

  function consoleError(message_id) {
    console.error(stackRead(message_id));
  }

  function throwException(value_ix) {
    throw stackRead(value_ix);
  }

  const wasm = await WebAssembly.instantiate(
    dida_wasm_bytes,
    {
      env: {
        jsTypeOf: jsTypeOf,
        createUndefined: createUndefined,
        createBool: stackPush,
        createU32: stackPush,
        createI32: stackPush,
        createI64: stackPush,
        createF64: stackPush,
        createString: createString,
        createObject: createObject,
        createArray: createArray,
        createRefCounted: createRefCounted,
        getU32: stackRead,
        getI32: stackRead,
        getI64: stackRead,
        getF64: stackRead,
        getStringLength: getStringLength,
        getStringInto: getStringInto,
        getRefCounted: getRefCounted,
        getArrayLength: getArrayLength,
        getElement: getElement,
        setElement: setElement,
        getProperty: getProperty,
        setProperty: setProperty,
        callFunction: callFunction,
        consoleLog: consoleLog,
        consoleError: consoleError,
        throwException: throwException,
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