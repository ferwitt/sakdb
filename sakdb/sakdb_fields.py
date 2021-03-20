import datetime
import hashlib
import json
import uuid
from typing import List, Optional

PAYLOAD_SEPARATOR = "&"


def payload_md5(payload: str) -> str:
    hash_object = hashlib.md5(bytes(payload, "utf-8"))
    return "md5:" + hash_object.hexdigest()


class SakDbField:
    ts: float
    key: str
    crc: str
    payload: str

    def __init__(
        self,
        payload: str,
        ts: Optional[float] = None,
        key: Optional[str] = None,
        crc: Optional[str] = None,
    ) -> None:
        self.payload = payload

        if key is None:
            self.key = uuid.uuid4().hex
        else:
            self.key = key

        if ts is None:
            self.ts = datetime.datetime.utcnow().timestamp()
        else:
            self.ts = ts

        if crc is not None:
            self.crc = crc
        else:
            # TODO: Compute this crc using the payload
            self.crc = payload_md5(payload)


class SakDbFields:
    fields: List[SakDbField]

    def __init__(self, *fields: SakDbField) -> None:
        self.fields: List[SakDbFields] = list(fields)

    def get_by_key(self, key: str) -> Optional[SakDbField]:
        for field in self.fields:
            if field.key == key:
                return field
        return None

    def get_keys(self) -> List[str]:
        ret = []
        for field in self.fields:
            ret.append(field.key)
        return ret

    def drop_by_key_prefix(self, key_prefix: str) -> None:
        new_fields = [f for f in self.fields if not f.key.startswith(key_prefix)]
        self.fields = new_fields


def sakdb_loads(data: str) -> Optional[SakDbFields]:
    ret = None

    for line_idx, line in enumerate(data.splitlines()):
        line = line.strip()
        if not line:
            continue

        header, payload = line.split(PAYLOAD_SEPARATOR, maxsplit=1)

        parsed_header = json.loads(header)
        parsed_payload = json.loads(payload)

        if ret is None:
            ret = SakDbFields()
        ret.fields.append(
            SakDbField(
                ts=parsed_header["t"],
                key=parsed_header["k"],
                crc=parsed_header["c"],
                payload=parsed_payload,
            )
        )

    return ret


def sakdb_dumps(data: SakDbFields) -> str:
    ret = []

    for field in data.fields:
        header = json.dumps(
            {"t": field.ts, "k": field.key, "c": field.crc}, separators=(",", ":")
        )
        payload = json.dumps(field.payload, separators=(",", ":"))

        if PAYLOAD_SEPARATOR in header:
            raise Exception(
                f'It is not allowed to have the "{PAYLOAD_SEPARATOR}" in the header'
            )

        ret.append(header + PAYLOAD_SEPARATOR + payload)

    # Add an extra new line to make the diffs easier.
    return "\n".join(ret) + "\n"


def merge(
    base: Optional[SakDbFields],
    ours: Optional[SakDbFields],
    theirs: Optional[SakDbFields],
) -> SakDbFields:

    new_fields: List[SakDbField] = []

    # TODO(witt): Check everything that was removed.
    # If there is a common base.
    if base and ours and theirs:

        # Merge mode: Just accept the most recent.
        all_keys = set(ours.get_keys()).union(set(theirs.get_keys()))
        for key in sorted(all_keys):
            _ours = ours.get_by_key(key)
            _theirs = theirs.get_by_key(key)

            if (_ours is not None) and (_theirs is not None):
                # If both are available, just choose the newest.
                if _ours.ts > _theirs.ts:
                    new_fields.append(_ours)
                else:
                    new_fields.append(_theirs)
            elif _ours is not None:
                new_fields.append(_ours)
            elif _theirs is not None:
                new_fields.append(_theirs)

    # If not common base.
    if (base is None) and ours and theirs:

        # Merge mode: Just accept the most recent.
        all_keys = set(ours.get_keys()).union(set(theirs.get_keys()))
        for key in sorted(all_keys):
            _ours = ours.get_by_key(key)
            _theirs = theirs.get_by_key(key)

            if (_ours is not None) and (_theirs is not None):
                # If both are available, just choose the newest.
                if _ours.ts > _theirs.ts:
                    new_fields.append(_ours)
                else:
                    new_fields.append(_theirs)
            elif _ours is not None:
                new_fields.append(_ours)
            elif _theirs is not None:
                new_fields.append(_theirs)

    # If only ours.
    if (base is None) and ours and (theirs is None):
        new_fields = ours.fields

    # If only theirs.
    if (base is None) and (ours is None) and theirs:
        new_fields = theirs.fields

    return SakDbFields(*new_fields)
