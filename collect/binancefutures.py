import asyncio
import json
import logging
import os
import time
import urllib.parse

import aiohttp
from aiohttp import ClientSession, WSMsgType
from yarl import URL


class BinanceFutures:
    def __init__(self, queue, symbols, timeout=7):
        with open(os.path.join(os.path.dirname(__file__), 'binance.json')) as f:
            self.config = json.load(f)['binancefutures']
        self.symbols = symbols
        self.client = None  # Will be initialized in connect()
        self.closed = False
        self.pending_messages = {}
        self.prev_u = {}
        self.timeout = timeout
        self.keep_alive = None
        self.queue = queue
        self.retries = 0  # Add retries counter initialization

    async def __on_message(self, raw_message):
        timestamp = time.time()
        message = json.loads(raw_message)
        # logging.debug(message)
        stream = message['stream']
        tokens = stream.split('@')
        if tokens[1] == 'depth':
            symbol = tokens[0]
            data = message['data']
            u = data['u']
            pu = data['pu']
            prev_u = self.prev_u.get(symbol)
            if prev_u is None or pu != prev_u:
                pending_messages = self.pending_messages.get(symbol)
                if pending_messages is None:
                    logging.warning('Mismatch on the book. prev_update_id=%s, pu=%s' % (prev_u, pu))
                    asyncio.create_task(self.__get_marketdepth_snapshot(symbol))
                    self.pending_messages[symbol] = pending_messages = []
                pending_messages.append((message, raw_message))
            else:
                self.queue.put((symbol, timestamp, raw_message))
                self.prev_u[symbol] = u
        elif tokens[1] == 'aggTrade':
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))
        elif tokens[1] == 'trade':
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))
        elif tokens[1] == 'bookTicker':
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))
        elif tokens[1] == 'markPrice':
            symbol = tokens[0]
            self.queue.put((symbol, timestamp, raw_message))

    async def __keep_alive(self):
        while not self.closed:
            try:
                await asyncio.sleep(5)
                await self.ws.pong()
            except asyncio.CancelledError:
                return
            except:
                logging.exception('Failed to keep alive.')
                return

    async def __curl(self, path, query=None, timeout=None, verb=None, rethrow_errors=None, max_retries=None):
        if timeout is None:
            timeout = self.timeout

        # Default to POST if data is attached, GET otherwise
        if not verb:
            verb = 'POST' if query else 'GET'

        # By default don't retry POST or PUT. Retrying GET/DELETE is okay because they are idempotent.
        # In the future we could allow retrying PUT, so long as 'leavesQty' is not used (not idempotent),
        # or you could change the clOrdID (set {"clOrdID": "new", "origClOrdID": "old"}) so that an amend
        # can't erroneously be applied twice.
        if max_retries is None:
            max_retries = 0 if verb in ['POST', 'PUT'] else 3

        if query is None:
            query = {}
        query['timestamp'] = str(int(time.time() * 1000) - 1000)
        query = urllib.parse.urlencode(query)
        # query = query.replace('%27', '%22')

        def exit_or_throw(e):
            if rethrow_errors:
                raise e
            else:
                exit(1)

        def retry():
            self.retries += 1
            if self.retries > max_retries:
                raise Exception("Max retries on %s (%s) hit, raising." % (path, json.dumps(query or '')))
            return self.__curl(path, query, timeout, verb, rethrow_errors, max_retries)

        # Make the request
        try:
            url = URL('https://fapi.binance.com/fapi%s?%s' % (path, query), encoded=True)
            logging.info("sending req to %s: %s" % (url, json.dumps(query or query or '')))
            response = await self.client.request(verb, url, timeout=timeout)
            # Make non-200s throw
            response.raise_for_status()

        except aiohttp.ClientResponseError as e:
            # 429, ratelimit; cancel orders & wait until X-RateLimit-Reset
            if e.status == 429:
                logging.error("Ratelimited on current request. Sleeping, then trying again. Try fewer " + "Request: %s \n %s" % (url, json.dumps(query)))
                logging.warning("Canceling all known orders in the meantime.")

                #logging.error("Your ratelimit will reset at %s. Sleeping for %d seconds." % (reset_str, to_sleep))
                to_sleep = 5
                logging.error("Sleeping for %d seconds." % (to_sleep))
                time.sleep(to_sleep)

                # Retry the request.
                return await retry()

            elif e.status == 502:
                logging.warning("Unable to contact the Binance Futures API (502), retrying. " + "Request: %s \n %s" % (url, json.dumps(query)))
                await asyncio.sleep(3)
                return await retry()

            # 503 - Binance Futures temporary downtime, likely due to a deploy. Try again
            elif e.status == 503:
                logging.warning("Unable to contact the Binance Futures API (503), retrying. " + "Request: %s \n %s" % (url, json.dumps(query)))
                await asyncio.sleep(3)
                return await retry()

            elif e.status == 400:
                pass
            # If we haven't returned or re-raised yet, we get here.
            logging.error("Unhandled Error: %s: %s" % (e, e.message))
            logging.error("Endpoint was: %s %s: %s" % (verb, path, json.dumps(query)))
            exit_or_throw(e)

        except asyncio.TimeoutError as e:
            # Timeout, re-run this request
            logging.warning("Timed out on request: %s (%s), retrying..." % (path, json.dumps(query or '')))
            return await retry()

        except aiohttp.ClientConnectionError as e:
            logging.warning("Unable to contact the Binance Futures API (%s). Please check the URL. Retrying. " + "Request: %s %s \n %s" % (e, url, json.dumps(query)))
            await asyncio.sleep(1)
            return await retry()

        # Reset retry counter on success
        self.retries = 0

        return await response.json()

    async def connect(self):
        try:
        # Initialize client session if not already done
            if self.client is None:
                self.client = aiohttp.ClientSession(headers={'Content-Type': 'application/json'})

            stream = '/'.join([f"{symbol}{suffix}" 
                             for symbol in self.symbols 
                             for suffix in self.config['streams']])
            url = f"{self.config['ws_url']}?streams={stream}"
            async with ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    logging.info('WS Connected.')
                    self.ws = ws
                    self.keep_alive = asyncio.create_task(self.__keep_alive())
                    async for msg in ws:
                        if msg.type == WSMsgType.TEXT:
                            await self.__on_message(msg.data)
                        elif msg.type == WSMsgType.BINARY:
                            pass
                        elif msg.type == WSMsgType.PING:
                            await self.ws.pong()
                        elif msg.type == WSMsgType.PONG:
                            await self.ws.ping()
                        elif msg.type == WSMsgType.ERROR:
                            exc = ws.exception()
                            raise exc if exc is not None else Exception
        except:
            logging.exception('WS Error')
        finally:
            logging.info('WS Disconnected.')
            if self.keep_alive is not None:
                self.keep_alive.cancel()
                await self.keep_alive
            self.ws = None
            self.keep_alive = None

    async def close(self):
        self.closed = True
        await self.ws.close()
        await self.client.close()
        await asyncio.sleep(1)

    async def __get_marketdepth_snapshot(self, symbol):
        data = await self.__curl(verb='GET', path='/v1/depth', query={'symbol': symbol, 'limit': 1000})
        self.queue.put((symbol, time.time(), json.dumps(data)))
        lastUpdateId = data['lastUpdateId']
        self.prev_u[symbol] = None
        # Process the pending messages.
        prev_u = None
        while prev_u is None:
            pending_messages = self.pending_messages.get(symbol)
            timestamp = time.time()
            while pending_messages:
                message, raw_message = pending_messages.pop(0)
                data = message['data']
                u = data['u']
                U = data['U']
                pu = data['pu']
                # https://binance-docs.github.io/apidocs/futures/en/#how-to-manage-a-local-order-book-correctly
                # The first processed event should have U <= lastUpdateId AND u >= lastUpdateId
                if (u < lastUpdateId or U > lastUpdateId) and prev_u is None:
                    continue
                if prev_u is not None and pu != prev_u:
                    logging.warning('UpdateId does not match. symbol=%s, prev_update_id=%d, pu=%d' % (symbol, prev_u, pu))
                self.queue.put((symbol, timestamp, raw_message))
                self.prev_u[symbol] = prev_u = u
            if prev_u is None:
                await asyncio.sleep(0.5)
        self.pending_messages[symbol] = None
        logging.warning('The book is initialized. symbol=%s, prev_update_id=%d' % (symbol, prev_u))
