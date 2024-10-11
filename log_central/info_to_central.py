import pandas as pd
import datetime as dt


def client_to_central(client, engine_local, engine_central, current_date):
    print('Getting data from ',client)

    """
    function to send all log information to our central server
    params: 
    client: name of the client we want to appear in the email
    engine_local: local engine
    engine_central: remote engine
    current_date: data local
    """

    if engine_central == '' or engine_central == '':
        return 'Please, provide the engine information of the databases'


    autoloader = None
    file = None
    quarantines = None
    pipeFlow = None

    autoloader = pd.read_sql_query(f"""
        select 
            max(startDate) as startDate, max(totalTime) as totalTime, sum(totalFiles) as totalFiles, sum(totalRows) as totalRows, sum(warningRowsTotal) as warningRowsTotal, sum(quarantineRowsTotal) as quarantineRowsTotal, 
            (select count(*) from Symphony_LoadRecalculateLog where isLoadSuccessful = 1 and startDate > '{current_date}') as successLoad,
            (select count(*) from Symphony_LoadRecalculateLog where isLoadSuccessful = 0 and startDate > '{current_date}') as failLoad
        from Symphony_LoadRecalculateLog where startDate > '{current_date}' group by cast(startDate as date)
    """,engine_local)
    autoloader

    if autoloader.shape[0] > 0 and autoloader.iloc[0].totalTime == None: quarantines = pd.DataFrame(data=[],columns=['Error']) #If still processing Onebeat L&R, don't query errors (it get stucked)
    else:
        quarantines = pd.read_sql_query(f"""
            declare @dateLogOnebeat nvarchar(100) = cast('{current_date}' as date)
            select StockLocations_Error as Error from (
                select distinct 'Locations - ' + replace(replace(quarantineReason,substring(quarantineReason,CHARINDEX('<',quarantineReason),CHARINDEX('>',quarantineReason)),''),substring(quarantineReason,CHARINDEX('<',quarantineReason,len(quarantineReason)/2),CHARINDEX('>',quarantineReason,len(quarantineReason)/2)),'')as StockLocations_Error from Symphony_InputNewStockLocationQuarantine where type = 'ERROR' and cast(loadingDate as date) = @dateLogOnebeat
                union all select distinct 'Status - ' + replace(replace(quarantineReason,substring(quarantineReason,CHARINDEX('<',quarantineReason),CHARINDEX('>',quarantineReason)),''),substring(quarantineReason,CHARINDEX('<',quarantineReason,len(quarantineReason)/2),CHARINDEX('>',quarantineReason,len(quarantineReason)/2)),'')as Status_Error from Symphony_InputStatusQuarantine where type = 'ERROR' and cast(loadingDate as date) = @dateLogOnebeat
                union all select distinct 'Transactions - ' + replace(replace(quarantineReason,substring(quarantineReason,CHARINDEX('<',quarantineReason),CHARINDEX('>',quarantineReason)),''),substring(quarantineReason,CHARINDEX('<',quarantineReason,len(quarantineReason)/2),CHARINDEX('>',quarantineReason,len(quarantineReason)/2)),'')as Transactions_Error from Symphony_InputTransactionQuarantine where type = 'ERROR' and cast(loadingDate as date) = @dateLogOnebeat
                union all select distinct 'Mtsskus - ' + replace(replace(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE (quarantineReason,'0', ''),'1', ''),'2', ''),'3', ''),'4', ''),'5', ''),'6', ''),'7', ''),'8', ''),'9', ''),substring(quarantineReason,CHARINDEX('<',quarantineReason),CHARINDEX('>',quarantineReason)),''),substring(quarantineReason,CHARINDEX('<',quarantineReason,len(quarantineReason)/2),CHARINDEX('>',quarantineReason,len(quarantineReason)/2)),'')as Mtsskus_Error from Symphony_InputNewSkuQuarantine where type = 'ERROR' and cast(loadingDate as date) = @dateLogOnebeat
            ) as LogOnebeat
        """,engine_local)

    if autoloader.shape[0] > 0: files = pd.read_sql_query(f"""select top (select sum(totalFiles) from symphony_LoadRecalculateLog where startDate > '{current_date}') inputObjectName from Symphony_LoadRecalculateInput ORDER BY ID DESC""",engine_local)
    else: files = pd.DataFrame(columns=['inputObjectName'])

    # pipeFlow = pd.read_sql_query(f"select Process, Value from Log_integration where cast(Date as date) = cast('{current_date}' as date) order by date",engine_local)
    pipeFlow = pd.read_sql_query(f"""
        select max(a.date) as date, a.Process, a.val from (
            select distinct date, Process, first_value(Value) over(partition by Process order by date desc) as val from Log_Integration 
            where cast(Date as date) = cast('{current_date}' as date) group by date, Process, Value
        ) as a group by Process, val order by date""",engine_local)
    if pipeFlow.shape[0] > 0: pipeFlow['log'] = pipeFlow.apply(lambda x: f"""<p style="display:inline;font-size:70%; {'color:green' if x.val == 'Sucess' else 'color:red'}"> {x.Process} = {x.val} - {x.date.time().strftime('%H:%M')}""",axis=1)
    else: pipeFlow = pd.DataFrame(columns=['log'])

    #====================# Assembly
    log = pd.DataFrame(columns=['date','client','startDate','totalTime','totalFiles','totalRows','warningRowsTotal','quarantineRowsTotal','successLoad','failLoad','quarantineErrors','importedFiles','integrationStatus'],index=[0])
    log['date'] = dt.datetime.strftime(dt.datetime.today(),'20%y-%m-%d %H:%M:%S')
    log['client'] = client

    log['startDate'] = autoloader.startDate[0] if autoloader.shape[0] > 0 else pd.to_datetime('1900-01-01')
    log['totalTime'] = autoloader.totalTime[0] if autoloader.shape[0] > 0 else pd.to_datetime('1900-01-01')
    log['totalFiles'] = autoloader.totalFiles[0] if autoloader.shape[0] > 0 else 0
    log['totalRows'] = autoloader.totalRows[0] if autoloader.shape[0] > 0 else 0
    log['warningRowsTotal'] = autoloader.warningRowsTotal[0] if autoloader.shape[0] > 0 else 0
    log['quarantineRowsTotal'] = autoloader.quarantineRowsTotal[0] if autoloader.shape[0] > 0 else 0
    log['successLoad'] = autoloader.successLoad[0] if autoloader.shape[0] > 0 else 0
    log['failLoad'] = autoloader.failLoad[0] if autoloader.shape[0] > 0 else 0

    log['quarantineErrors'] = str(quarantines.Error.to_list())
    log['importedFiles'] = str(files.inputObjectName.to_list())
    log['integrationStatus'] = str(pipeFlow.log.to_list())

    log.to_sql('integrationsLog',engine_central,if_exists='append',index=False)

#====================##====================##====================##====================##====================#