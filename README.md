# log_central

Module created to take the information of the integrations and send to our central database.

Use env variables of the connection to local and remote database.


# COMO USAR
```python

from log_central.info_to_central import client_to_central

try:
    client_to_central(client, ENGINE_ONEBEAT, ENGINE_CENTRAL, CURRENT_DATE)
except Exception as eGet: print(client,': Error in getClientInfoFromCentralDB: ',eGet)

