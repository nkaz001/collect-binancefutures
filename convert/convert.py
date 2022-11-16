import argparse
import gzip
import json
import os

import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('src_file')
    parser.add_argument('dst_path')
    parser.add_argument('-s', '--snapshot')
    parser.add_argument('-f', '--full', action='store_true')
    parser.add_argument('-c', '--correct', action='store_true')

    args = parser.parse_args()

    src_file = args.src_file
    ext = os.path.splitext(src_file)[1]
    if ext == '.gz':
        filename = os.path.basename(os.path.splitext(os.path.splitext(src_file)[0])[0])
        open_func = gzip.open
    elif ext == '.dat':
        filename = os.path.basename(os.path.splitext(src_file)[0])
        open_func = open
    else:
        raise ValueError

    dst_file = os.path.join(args.dst_path, filename + '.pkl')
    snapshot_dst_file = os.path.join(args.dst_path, filename + '.snapshot.pkl')
    snapshot_src_file = args.snapshot
    correct_exch_timestamp = args.correct

    bid_depth = {}
    ask_depth = {}
    if snapshot_src_file is not None:
        snapshot_df = pd.read_pickle(snapshot_src_file, compression='gzip')
        for row_num, row in snapshot_df.iterrows():
            if row['side'] == 1:
                bid_depth[str(row['price'])] = str(row['qty'])
            elif row['side'] == -1:
                ask_depth[str(row['price'])] = str(row['qty'])

    prev_exch_timestamp = 0
    rows = []
    with open_func(src_file, 'r') as f:
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
                    exch_timestamp = int(transaction_time) * 1000
                    if correct_exch_timestamp and exch_timestamp < prev_exch_timestamp:
                        exch_timestamp = prev_exch_timestamp
                    prev_exch_timestamp = exch_timestamp
                    rows.append([2, exch_timestamp, local_timestamp, side, float(price), float(qty)])
                elif evt == 'depthUpdate':
                    # event_time = data['E']
                    transaction_time = data['T']
                    bids = data['b']
                    asks = data['a']
                    exch_timestamp = int(transaction_time) * 1000
                    if correct_exch_timestamp and exch_timestamp < prev_exch_timestamp:
                        exch_timestamp = prev_exch_timestamp
                    prev_exch_timestamp = exch_timestamp
                    rows += [[1, exch_timestamp, local_timestamp, 1, float(bid[0]), float(bid[1])] for bid in bids]
                    rows += [[1, exch_timestamp, local_timestamp, -1, float(ask[0]), float(ask[1])] for ask in asks]
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
                elif evt == 'markPriceUpdate' and args.full:
                    # event_time = data['E']
                    transaction_time = data['T']
                    index = data['i']
                    mark_price = data['p']
                    # est_settle_price = data['P']
                    funding_rate = data['r']
                    rows.append([100, prev_exch_timestamp, local_timestamp, 0, float(index), 0])
                    rows.append([101, prev_exch_timestamp, local_timestamp, 0, float(mark_price), 0])
                    rows.append([102, prev_exch_timestamp, local_timestamp, 0, float(funding_rate), 0])
                elif evt == 'bookTicker' and args.full:
                    # event_time = data['E']
                    transaction_time = data['T']
                    bid_price = data['b']
                    bid_qty = data['B']
                    ask_price = data['a']
                    ask_qty = data['A']
                    exch_timestamp = int(transaction_time) * 1000
                    if correct_exch_timestamp and exch_timestamp < prev_exch_timestamp:
                        exch_timestamp = prev_exch_timestamp
                    prev_exch_timestamp = exch_timestamp
                    rows.append([103, exch_timestamp, local_timestamp, 1, float(bid_price), float(bid_qty)])
                    rows.append([104, exch_timestamp, local_timestamp, -1, float(ask_price), float(ask_qty)])
            else:
                # snapshot
                # event_time = msg['E']
                transaction_time = message['T']
                bids = message['bids']
                asks = message['asks']
                bid_clear_upto = float(bids[-1][0])
                ask_clear_upto = float(asks[-1][0])
                exch_timestamp = int(transaction_time) * 1000
                if correct_exch_timestamp and exch_timestamp < prev_exch_timestamp:
                    exch_timestamp = prev_exch_timestamp
                prev_exch_timestamp = exch_timestamp
                # clear the existing market depth upto the prices in the snapshot.
                rows.append([3, exch_timestamp, local_timestamp, 1, bid_clear_upto, 0])
                rows.append([3, exch_timestamp, local_timestamp, -1, ask_clear_upto, 0])
                for bid in list(bid_depth.keys()):
                    if float(bid) > float(bid_clear_upto) or bid == bids[-1][0]:
                        del bid_depth[bid]
                for ask in list(ask_depth.keys()):
                    if float(ask) < float(ask_clear_upto) or ask == asks[-1][0]:
                        del ask_depth[ask]
                # insert the snapshot.
                rows += [[4, exch_timestamp, local_timestamp, 1, float(bid[0]), float(bid[1])] for bid in bids]
                rows += [[4, exch_timestamp, local_timestamp, -1, float(ask[0]), float(ask[1])] for ask in asks]
                for bid in bids:
                    bid_depth[bid[0]] = bid[1]
                for ask in asks:
                    ask_depth[ask[0]] = ask[1]
    df = pd.DataFrame(rows, columns=['event', 'exch_timestamp', 'local_timestamp', 'side', 'price', 'qty'])
    df.to_pickle(dst_file, compression='gzip')

    snapshot = []
    snapshot += [[4, exch_timestamp, local_timestamp, 1, float(bid), float(qty)]
                 for bid, qty in sorted(bid_depth.items(), key=lambda v: -float(v[0]))]
    snapshot += [[4, exch_timestamp, local_timestamp, -1, float(ask), float(qty)]
                 for ask, qty in sorted(ask_depth.items(), key=lambda v: float(v[0]))]
    snapshot_df = pd.DataFrame(snapshot, columns=['event', 'exch_timestamp', 'local_timestamp', 'side', 'price', 'qty'])
    snapshot_df.to_pickle(snapshot_dst_file, compression='gzip')

    print('Done. rows=%d, filename=%s' % (len(df), dst_file))
