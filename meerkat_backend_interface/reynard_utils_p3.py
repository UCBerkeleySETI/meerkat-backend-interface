# Local version of a subset of the utilities from the 
# Reynard submodule (MIT license, (c) Ewan Barr, 2017)
# for compatibility purposes. 
# These functions are from  https://github.com/ewanbarr/reynard
# at 5c01a09146f4782d350c258c415661896102036c
import json
import re

"""
This regex is used to resolve escaped characters
in KATCP messages
"""
ESCAPE_SEQUENCE_RE = re.compile(r'''
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )''', re.UNICODE | re.VERBOSE)

def unescape_string(s):
    def decode_match(match):
        return codecs.decode(match.group(0), 'unicode-escape')
    return ESCAPE_SEQUENCE_RE.sub(decode_match, s)

def unescape_string(s):
    def decode_match(match):
        return codecs.decode(match.group(0), 'unicode-escape')
    return ESCAPE_SEQUENCE_RE.sub(decode_match, s)

def unpack_dict(x):
    try:
        return json.loads(decode_katcp_message(x))
    except:
        return json.loads(x.replace("\_"," ").replace("\n","\\n"))

def decode_katcp_message(s):
    """
    Render a katcp message human readable.
    
    Args:
        s (str): katcp message.

    Returns:
        Human readable string.
    """
    return unescape_string(s).replace("\_", " ")

