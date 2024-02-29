# import networkx as nx
from collections import defaultdict
from itertools import groupby
import pybgpstream

stream = pybgpstream.BGPStream(
    # Consider this time interval:
    # Sat, 01 Aug 2015 7:50:00 GMT -  08:10:00 GMT
    from_time="2023-08-01 07:00:00", until_time="2023-08-01 07:02:00",
    collectors=["rrc00"],
    record_type="ribs",
)
# Create a new BGPStream instance and a reusable BGPRecord instance
# stream = pybgpstream.BGPStream(from_time="2015-08-01 07:50:00", until_time="2015-08-01 08:10:00",)

# Consider BGP updates
stream.add_filter('record-type', 'updates')

stream.add_filter('ipversion', '4')

metadata = set()
for rec in stream.records():
    print(rec)
    for elem in rec:
        # Get the peer ASn
        peer = str(elem.peer_asn)
        if elem.type == 'A':  # Announcement type
        # Get the array of ASns in the AS path and remove repeatedly prepended ASns
            hops = [k for k, g in groupby(elem.fields['as-path'].split(" "))]
            # print(f"{peer}->{hops}")

