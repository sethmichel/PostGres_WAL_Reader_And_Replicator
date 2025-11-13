from typing import List, Dict, Any

'''
This is basically a test file

1) when we get a batch of decoded WAL records, they come here
2) we send them to teh sink by just printing them to teh console 
3) if we send everything to teh sink and they apply correctly we reply with an acknolodgement 
4) this acknolodgement will trigger teh last applied lsn to be saved to the lsn table
'''

# events: List[Dict[str, Any]]
def Apply_Stdout(events):
    for ev in events:
        print(ev)
