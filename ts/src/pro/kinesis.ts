//  ---------------------------------------------------------------------------

import kinesisRest from '../kinesis.js';
import { ArrayCache } from '../base/ws/Cache.js';
import type { Int, Trade, OrderBook, Ticker, Market, Dict } from '../base/types.js';
import Client from '../base/ws/Client.js';

//  ---------------------------------------------------------------------------

export default class kinesis extends kinesisRest {
    describe (): any {
        return this.deepExtend (super.describe (), {
            'has': {
                'ws': true,
                'watchTicker': true,
                'watchTickers': false,
                'watchTrades': true,
                'watchTradesForSymbols': false,
                'watchMyTrades': false,
                'watchOrders': false,
                'watchOrderBook': true,
                'watchOHLCV': false,
            },
            'urls': {
                'api': {
                    'ws': 'ws://127.0.0.1:8080',
                },
            },
            'options': {
            },
            'streaming': {
            },
            'exceptions': {
            },
        });
    }

    async watchTicker (symbol: string, params = {}): Promise<Ticker> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const url = this.urls['api']['ws'];
        const messageHash = 'ticker:' + symbol;
        const subscribe: Dict = {
            'event': 'subscribe',
            'channel': 'ticker',
            'symbol': symbol,
        };
        const request = this.deepExtend (subscribe, params);
        return await this.watch (url, messageHash, request, messageHash);
    }

    async watchTrades (symbol: string, since: Int = undefined, limit: Int = undefined, params = {}): Promise<Trade[]> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const url = this.urls['api']['ws'];
        const messageHash = 'trades:' + symbol;
        const subscribe: Dict = {
            'event': 'subscribe',
            'channel': 'trades',
            'symbol': symbol,
        };
        const request = this.deepExtend (subscribe, params);
        const trades = await this.watch (url, messageHash, request, messageHash);
        if (this.newUpdates) {
            limit = trades.getLimit (symbol, limit);
        }
        return this.filterBySinceLimit (trades, since, limit, 'timestamp', true);
    }

    async watchOrderBook (symbol: string, limit: Int = undefined, params = {}): Promise<OrderBook> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const url = this.urls['api']['ws'];
        const messageHash = 'orderbook:' + symbol;
        const subscribe: Dict = {
            'event': 'subscribe',
            'channel': 'orderbook',
            'symbol': symbol,
        };
        const request = this.deepExtend (subscribe, params);
        const orderbook = await this.watch (url, messageHash, request, messageHash);
        return orderbook.limit ();
    }

    handleMessage (client: Client, message) {
        if (message === '') {
            return;
        }
        if (typeof message === 'string') {
            try {
                message = JSON.parse (message);
            } catch (e) {
                return;
            }
        }
        const event = this.safeString (message, 'event');
        if (event === 'subscribed') {
            return;
        }
        const channel = this.safeString (message, 'channel');
        if (channel === 'ticker') {
            this.handleTicker (client, message);
        } else if (channel === 'orderbook') {
            this.handleOrderBook (client, message);
        } else if (channel === 'trades') {
            this.handleTrades (client, message);
        }
    }

    handleTicker (client: Client, message) {
        const symbol = this.safeString (message, 'symbol');
        const market = this.market (symbol);
        const data = this.safeDict (message, 'data', {});
        const last = this.safeString (data, 'last');
        const timestamp = this.safeInteger (message, 'timestamp', this.milliseconds ());
        const ticker = this.safeTicker ({
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': this.iso8601 (timestamp),
            'high': undefined,
            'low': undefined,
            'bid': this.safeString (data, 'bid'),
            'bidVolume': undefined,
            'ask': this.safeString (data, 'ask'),
            'askVolume': undefined,
            'vwap': undefined,
            'open': undefined,
            'close': last,
            'last': last,
            'previousClose': undefined,
            'change': undefined,
            'percentage': undefined,
            'average': undefined,
            'baseVolume': this.safeString (data, 'volume'),
            'quoteVolume': undefined,
            'info': message,
        }, market);
        this.tickers[symbol] = ticker;
        const messageHash = 'ticker:' + symbol;
        client.resolve (ticker, messageHash);
    }

    handleOrderBook (client: Client, message) {
        const symbol = this.safeString (message, 'symbol');
        const messageHash = 'orderbook:' + symbol;
        const timestamp = this.safeInteger (message, 'timestamp', this.milliseconds ());
        const data = this.safeDict (message, 'data', {});
        if (!(symbol in this.orderbooks)) {
            this.orderbooks[symbol] = this.orderBook ({});
        }
        const orderbook = this.orderbooks[symbol];
        const snapshot = this.parseOrderBook (data, symbol, timestamp, 'bids', 'asks', 0, 1);
        orderbook.reset (snapshot);
        client.resolve (orderbook, messageHash);
    }

    handleTrades (client: Client, message) {
        const symbol = this.safeString (message, 'symbol');
        const market = this.market (symbol);
        const data = this.safeList (message, 'data', []);
        let stored = this.safeValue (this.trades, symbol);
        if (stored === undefined) {
            const limit = this.safeInteger (this.options, 'tradesLimit', 1000);
            stored = new ArrayCache (limit);
            this.trades[symbol] = stored;
        }
        for (let i = 0; i < data.length; i++) {
            const trade = this.parseTrade (data[i], market);
            stored.append (trade);
        }
        this.trades[symbol] = stored;
        const messageHash = 'trades:' + symbol;
        client.resolve (stored, messageHash);
    }

    parseTrade (trade: Dict, market: Market = undefined): Trade {
        const timestamp = this.safeInteger (trade, 'timestamp');
        const id = this.safeString (trade, 'id');
        const price = this.safeString (trade, 'price');
        const amount = this.safeString (trade, 'amount');
        const side = this.safeString (trade, 'side');
        return this.safeTrade ({
            'id': id,
            'info': trade,
            'timestamp': timestamp,
            'datetime': this.iso8601 (timestamp),
            'symbol': market['symbol'],
            'order': undefined,
            'type': undefined,
            'side': side,
            'takerOrMaker': undefined,
            'price': price,
            'amount': amount,
            'cost': undefined,
            'fee': undefined,
        }, market);
    }
}
