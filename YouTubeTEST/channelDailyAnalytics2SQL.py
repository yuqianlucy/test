import datetime
from analytix import Analytics
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer

client = Analytics.with_secrets("../ignore/SOHclient_secret.json")

engine = create_engine('mysql+mysqldb://franklee:frank_2022%402022@184.105.241.89:3306/SOH_YouTube_Analysis')
con = engine.connect()

startDate = input('Retrieve daily channel statistics since: YYYY-MM-DD')
startDate = startDate.strip()

format = '%Y-%m-%d'
startDate = datetime.datetime.strptime(startDate, format).date()

report = client.daily_analytics(
          of=None,
          since=startDate,
          metrics='all'
        )

resultsDF = report.to_dataframe()

resultsDF.to_sql(name='''SOHChannelDailyReport''', con=engine, if_exists='append', index=False)