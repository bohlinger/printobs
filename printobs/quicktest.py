from utils import call_frost_api
from utils import get_frost_df
from datetime import datetime, timedelta

ed = datetime.now() + timedelta(hours=3)
sd = datetime.now() - timedelta(hours=12)
#s = 'ekofiskL'
s = 'scarabeo8'
#v = 'v0'
v = 'v1'

r = call_frost_api(sd,ed,s,v)


