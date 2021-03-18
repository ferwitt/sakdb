from pathlib import Path

import tempfile
import time
import json

import subprocess

from IPython import embed

from data_field import data_object_loads, data_object_dumps, DataObjectFields, DataObjectField, PAYLOAD_SEPARATOR, merge

import base64


def test_dumps():
    # Given.
    data = DataObjectFields('foo', [
        DataObjectField(ts=1.0, key="0000000000000000000000000000000000000000",
        #crc="0000000000000000000000000000000000000000", 
        payload="Hello world"
        )
        ])

    # When.
    data_str = data_object_dumps(data)

    # Then.
    assert data_str == (
            '{"t": "foo"}\n'
            '{"t": 1.0, "k": "0000000000000000000000000000000000000000",'
            ' "c": "md5:3e25960a79dbc69b674cd4ec67a72c62"}'
            + PAYLOAD_SEPARATOR +
            '"Hello world"'
            )



def test_loads():
    # Given.
    data = DataObjectFields('foo', [
        DataObjectField(ts=1.0, key="0000000000000000000000000000000000000000",
        crc="1111111111111111111111111111111111111111", 
        payload="Hello world"
        )
        ])
    data_str = data_object_dumps(data)

    # When.
    obj = data_object_loads(data_str)

    # Then.
    assert obj.type_name =='foo'
    assert len(obj.fields) == 1
    assert obj.fields[0].key =='0000000000000000000000000000000000000000'
    assert obj.fields[0].crc =="1111111111111111111111111111111111111111"
    assert obj.fields[0].payload =="Hello world"


def test_merge_no_common_base():

    base_data = None
    ours_data= ( '{"t": "str"}\n{"t": 1616074500.117626, "k": "name",'
            ' "c": "md5:4d498457eff880b7aadb6e620344a8e3"} || "\\"helloWorld\\""'
            )
    theirs_data = (
            '{"t": "str"}\n{"t": 1616074501.124153, "k": "name",'
            ' "c": "md5:8dd9a71fdf86e9f8551294356894b569"} || "\\"fooBar\\""'
            )


    ours = data_object_loads(ours_data)
    theirs = data_object_loads(theirs_data)

    merged = merge(None, ours, theirs)

    assert len(merged.fields) == 1

    assert 0


