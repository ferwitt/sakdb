from sakdb.sakdb_fields import (
    PAYLOAD_SEPARATOR,
    SakDbField,
    SakDbFields,
    merge,
    sakdb_dumps,
    sakdb_loads,
)


def test_dumps() -> None:
    # Given.
    data = SakDbFields(
        SakDbField(
            ts=1.0,
            key="0000000000000000000000000000000000000000",
            payload="Hello world",
        )
    )

    # When.
    data_str = sakdb_dumps(data)

    # Then.
    assert data_str == (
        '{"t":1.0,"k":"0000000000000000000000000000000000000000",'
        '"c":"md5:3e25960a79dbc69b674cd4ec67a72c62"}'
        + PAYLOAD_SEPARATOR
        + '"Hello world"\n'
    )


def test_loads() -> None:
    # Given.
    data = SakDbFields(
        SakDbField(
            ts=1.0,
            key="0000000000000000000000000000000000000000",
            crc="1111111111111111111111111111111111111111",
            payload="Hello world",
        )
    )
    data_str = sakdb_dumps(data)

    # When.
    obj = sakdb_loads(data_str)

    # Then.
    assert obj is not None
    assert len(obj.fields) == 1
    assert obj.fields[0].key == "0000000000000000000000000000000000000000"
    assert obj.fields[0].crc == "1111111111111111111111111111111111111111"
    assert obj.fields[0].payload == "Hello world"


def test_merge_no_common_base() -> None:

    ours_data = (
        '{"t":1616074500.117626,"k":"name",'
        '"c": "md5:4d498457eff880b7aadb6e620344a8e3"}&"\\"helloWorld\\""'
    )
    theirs_data = (
        '{"t":1616074501.124153,"k":"name",'
        '"c":"md5:8dd9a71fdf86e9f8551294356894b569"}&"\\"fooBar\\""'
    )

    ours = sakdb_loads(ours_data)
    theirs = sakdb_loads(theirs_data)

    merged = merge(None, ours, theirs)

    assert len(merged.fields) == 1
