import re
import lldb

def print_rust_str(debugger, command, result, internal_dict):
    mem_threshold = 16384
    var = command

    ci = debugger.GetCommandInterpreter()
    res = lldb.SBCommandReturnObject()

    ci.HandleCommand("po {}.vec.len".format(var), res)
    if not res.Succeeded():
        result.SetError("dbg-vis {}".format(res.GetError()))
        return

    read_len = int(res.GetOutput())

    if read_len > mem_threshold:
        result.SetError("Unable to read {} bytes (threshold = {})".format(
            read_len,
            mem_threshold
        ))
        return

    ci.HandleCommand(
        "me read -s1 -fa -c{} {}.vec.buf.ptr.pointer --force".format(
            read_len,
            var
        ),
        res
    )

    if not res.Succeeded():
        result.SetError("dbg-vis {}".format(res.GetError()))
        return

    output = res.GetOutput()

    hex_regex = r'0x(\d|[A-Fa-f])+'
    begin_regex = r'(^|\n){hex}: '.format(hex = hex_regex)
    output = re.sub(begin_regex, '', output)

    print(output, file=result)
