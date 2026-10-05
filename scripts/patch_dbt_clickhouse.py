import os
import dbt.adapters.clickhouse as pkg

p = os.path.join(os.path.dirname(pkg.__file__), "dbclient.py")
with open(p, "r", encoding="utf-8") as fp:
    content = fp.read()

target = "self._conn_settings.setdefault('lightweight_deletes_sync', '3')"
if target in content:
    content = content.replace(target, "# self._conn_settings.setdefault('lightweight_deletes_sync', '3')")
    with open(p, "w", encoding="utf-8") as fp:
        fp.write(content)
    print("Successfully patched dbt-clickhouse dbclient.py!")
else:
    print("Already patched or target string not found.")
