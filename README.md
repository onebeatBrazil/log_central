# get_log_to_central

Módulo criado para levar informações das integrações para o banco de dados central.

Usando as variáveis de ambiente dos ambientes locais e central, enviar os dados para o banco de dados que usa

# COMO USAR
```python

from log_central.info_to_central import client_info_central

try:
    client_info_central(client, ENGINE_ONEBEAT, ENGINE_CENTRAL, CURRENT_DATE)
except Exception as eGet: print(client,': Error in getClientInfoFromCentralDB: ',eGet)

