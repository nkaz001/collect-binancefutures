import os
import sys

import pandas as pd
import json


if __name__ == '__main__':
    src_file = sys.argv[1]
    dst_file = os.path.join(sys.argv[2], os.path.basename(os.path.splitext(sys.argv[1])[0]) + '.pkl')

    rows = []
    with open(src_file, 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            local_timestamp = int(line[:16])
            message = json.loads(line[17:])
            data = message.get('data')
            if data is not None:
                evt = data['e']
                if evt == 'trade':
                    # event_time = data['E']
                    transaction_time = data['T']
                    price = data['p']
                    qty = data['q']
                    side = -1 if data['m'] else 1  # trade initiator's side
                    rows.append([2, local_timestamp, int(transaction_time) * 1000, side, float(price), float(qty)])
                elif evt == 'depthUpdate':
                    # event_time = data['E']
                    transaction_time = data['T']
                    bids = data['b']
                    asks = data['a']
                    rows += [[1, local_timestamp, int(transaction_time) * 1000, 1, float(bid[0]), float(bid[1])]
                             for bid in bids]
                    rows += [[1, local_timestamp, int(transaction_time) * 1000, -1, float(ask[0]), float(ask[1])]
                             for ask in asks]
            else:
                # snapshot
                # event_time = msg['E']
                transaction_time = message['T']
                bids = message['bids']
                asks = message['asks']
                bid_clear_upto = float(bids[-1][0])
                ask_clear_upto = float(asks[-1][0])
                # clear the existing market depth upto the prices in the snapshot.
                rows.append([3, local_timestamp, int(transaction_time) * 1000, 1, bid_clear_upto, 0])
                rows.append([3, local_timestamp, int(transaction_time) * 1000, -1, ask_clear_upto, 0])
                # insert the snapshot.
                rows += [[4, local_timestamp, int(transaction_time) * 1000, 1, float(bid[0]), float(bid[1])]
                         for bid in bids]
                rows += [[4, local_timestamp, int(transaction_time) * 1000, -1, float(ask[0]), float(ask[1])]
                         for ask in asks]
    df = pd.DataFrame(rows, columns=['event', 'exch_timestamp', 'local_timestamp', 'side', 'price', 'qty'])
    df.to_pickle(dst_file, compression='gzip')
    print('Done. rows=%d, filename=%s' % (len(df), dst_file))
