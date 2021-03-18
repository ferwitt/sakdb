import re
import json
#from dataclasses import dataclass
from typing import List, Any, Optional
import uuid
import datetime
import hashlib

PAYLOAD_SEPARATOR = ' || '

def payload_md5(payload: str) -> str:
    hash_object = hashlib.md5(bytes(payload, 'utf-8'))
    return 'md5:' + hash_object.hexdigest()

#@dataclass
class DataObjectField:
    ts: float
    key: str
    crc: str
    payload: str

    def __init__(self, payload:str, ts=None, key=None, crc=None):
        self.ts = ts
        self.key = key
        self.payload = payload

        if key is None:
            self.key = uuid.uuid4().hex

        if ts is None:
            self.ts = datetime.datetime.utcnow().timestamp()

        if crc is not None:
            self.crc = crc
        else:
            # TODO: Compute this crc using the payload
            self.crc = payload_md5(payload)


#@dataclass
class DataObjectFields:
    # TODO(witt): Maybe I should convert everything to be a DataField. I can use the id to store metadata.
    type_name: str
    fields: List[DataObjectField]

    def __init__(self, type_name, fields):
        self.type_name = type_name
        self.fields = fields

    def get_by_key(self, key: str) -> Optional[DataObjectField]:
        for field in self.fields:
            if field.key == key:
                return field
        return None

    def get_keys(self) -> List[str]:
        ret = []
        for field in self.fields:
            ret.append(field.key)
        return ret

def data_object_loads(data: str) -> Optional[DataObjectFields]:
    ret = None

    for line_idx, line in enumerate(data.splitlines()):
        line = line.strip()
        if not line: continue

        if ret is None:
            parsed = json.loads(line)
            if 't' in parsed:
                ret = DataObjectFields(parsed['t'], [])
            else:
                break
        else:
            header, payload = line.split(PAYLOAD_SEPARATOR, maxsplit=1)

            parsed_header = json.loads(header)
            parsed_payload = json.loads(payload)

            ret.fields.append(DataObjectField(
                ts=parsed_header['t'],
                key=parsed_header['k'],
                crc=parsed_header['c'],
                payload=parsed_payload
                ))

    return ret

def data_object_dumps(data: DataObjectFields) -> str:
    ret = []

    ret.append(json.dumps({'t': data.type_name}))

    payload_vargs = {}

    for field in data.fields:
        ret.append(
            json.dumps({
                't': field.ts,
                'k': field.key,
                'c': field.crc
                })
            +PAYLOAD_SEPARATOR
            + json.dumps(field.payload)
            )

    return '\n'.join(ret)

def merge(base, ours, theirs):


    # If there is a common base, check everything that was removed.
    ## TODO(witt): Hadle the case that a field was deleted.

    # If not common base, just accept the most common.

    new_fields = set()

    if (base is None) and ours and theirs:

        all_keys = set(ours.get_keys()).union(set(theirs.get_keys()))

        for key in all_keys:
            _ours = ours.get_by_key(key)
            _theirs = theirs.get_by_key(key)

            if (_ours is not None) and (_theirs is not None):
                # If both are available, just choose the newest.
                if _ours.ts > _theirs.ts:
                    new_fields.add(_ours)
                else:
                    new_fields.add(_theirs)
            elif (_ours is not None):
                new_fields.add(_ours)
            elif (_theirs is not None):
                new_fields.add(_theirs)

    if (base is None) and ours and (theirs is None):
        new_fields = ours.fields

    if (base is None) and (ours is None) and theirs:
        new_fields = theirs.fields

    new_data_type = None
    if ours is not None:
        new_data_type = ours.type_name
    if theirs is not None:
        new_data_type = theirs.type_name

    return DataObjectFields(new_data_type, list(new_fields))


