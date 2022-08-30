import os
import sys

import pandas as pd
import json


if __name__ == '__main__':
    src_file = sys.argv[1]
    filename = os.path.basename(os.path.splitext(src_file)[0])
    dst_file = os.path.join(sys.argv[2], filename + '.pkl')
    snapshot_dst_file = os.path.join(sys.argv[2], filename + '.snapshot.pkl')
    snapshot_src_file = sys.argv[3] if len(sys.argv) == 4 else None

    bid_depth = {}
    ask_depth = {}
    if snapshot_src_file is not None:
        snapshot_df = pd.read_pickle(snapshot_src_file, compression='gzip')
        for row_num, row in snapshot_df.iterrows():
            if row['side'] == 1:
                bid_depth[str(row['price'])] = str(row['qty'])
            elif row['side'] == -1:
                ask_depth[str(row['price'])] = str(row['qty'])

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
                    rows += [[1, local_timestamp, int(transaction_time) * 1000, 1, float(bid[0]), float(bid[1])] for bid in bids]
                    rows += [[1, local_timestamp, int(transaction_time) * 1000, -1, float(ask[0]), float(ask[1])] for ask in asks]
                    for bid in bids:
                        if round(float(bid[1]) / 0.000001) == 0:
                            if bid[0] in bid_depth:
                                del bid_depth[bid[0]]
                        else:
                            bid_depth[bid[0]] = bid[1]
                    for ask in asks:
                        if round(float(ask[1]) / 0.000001) == 0:
                            if ask[0] in ask_depth:
                                del ask_depth[ask[0]]
                        else:
                            ask_depth[ask[0]] = ask[1]
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
                for bid in list(bid_depth.keys()):
                    if float(bid) > float(bid_clear_upto) or bid == bids[-1][0]:
                        del bid_depth[bid]
                for ask in list(ask_depth.keys()):
                    if float(ask) < float(ask_clear_upto) or ask == asks[-1][0]:
                        del ask_depth[ask]
                # insert the snapshot.
                rows += [[4, local_timestamp, int(transaction_time) * 1000, 1, float(bid[0]), float(bid[1])] for bid in bids]
                rows += [[4, local_timestamp, int(transaction_time) * 1000, -1, float(ask[0]), float(ask[1])] for ask in asks]
                for bid in bids:
                    bid_depth[bid[0]] = bid[1]
                for ask in asks:
                    ask_depth[ask[0]] = ask[1]
    df = pd.DataFrame(rows, columns=['event', 'exch_timestamp', 'local_timestamp', 'side', 'price', 'qty'])
    df.to_pickle(dst_file, compression='gzip')

    snapshot = []
    snapshot += [[4, local_timestamp, int(transaction_time) * 1000, 1, float(bid), float(qty)]
                 for bid, qty in sorted(bid_depth.items(), key=lambda v: -float(v[0]))]
    snapshot += [[4, local_timestamp, int(transaction_time) * 1000, -1, float(ask), float(qty)]
                 for ask, qty in sorted(ask_depth.items(), key=lambda v: float(v[0]))]
    snapshot_df = pd.DataFrame(snapshot, columns=['event', 'exch_timestamp', 'local_timestamp', 'side', 'price', 'qty'])
    snapshot_df.to_pickle(snapshot_dst_file, compression='gzip')

    print('Done. rows=%d, filename=%s' % (len(df), dst_file))
