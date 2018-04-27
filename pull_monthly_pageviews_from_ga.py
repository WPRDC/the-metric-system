import time
from collections import defaultdict, OrderedDict
import pprint
from datapusher import Datapusher
from pull_monthly_metric_from_ga import fetch_and_store_metric

import sys # These two lines are here to prevent a weird problem wherein
sys.excepthook = sys.__excepthook__ # a) the script would get stuck after
# failing to retrieve some data from Google Analytics after trying twice
# and b) would just hang, rather than raising an exception (as designed).
#http://stackoverflow.com/questions/12865637/why-doesnt-python-exit-from-a-raised-exception-when-executed-with-an-absolute-p


####### Get monthly pageviews data ####################################
    # Create entire dataset-pageviews dataset by looking at every month.
    # For every resource_id in the data.json file, run metric_by_month and upsert the results to the monthly-pageviews datastore.

    # One issue is that there are a lot more resources that come up with non-zero pageviews than resources that come up with non-zero downloads (like 799 vs. like maybe 400-450).

def main():
    from credentials_file import profile # The GA profile ID for data.wrpdc.org.
    from credentials_file import monthly_pageviews_resource_id # The relevant CKAN resource.
    store = True
    metric = 'pageviews'
    first_yearmonth = '201510'
    resource_id = monthly_pageviews_resource_id
    event = False
    #store = False
    metrics_name = OrderedDict([("ga:pageviews",'Pageviews')])

    limit = 0
    if limit > 0:
        store = False
    fetch_and_store_metric(metric,metrics_name,resource_id,store,event,first_yearmonth,limit)

if __name__ == '__main__':
  main()
