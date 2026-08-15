//  ---------------------------------------------------------------------------

import kinesisRest from '../kinesis.js';
import { ArrayCache } from '../base/ws/Cache.js';
import { ExchangeError } from '../base/errors.js';
import type { Int, Trade, OrderBook, Ticker, Market, Dict, Strings, Tickers } from '../base/types.js';
import Client from '../base/ws/Client.js';

//  ---------------------------------------------------------------------------

export default class kinesis extends kinesisRest {
    override describe (): any {
        return this.deepExtend (super.describe (), {
            'has': {
                'ws': true,
                'watchTicker': true,
                'watchTickers': true,
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

    override async watchTicker (symbol: string, params = {}): Promise<Ticker> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const url = this.safeString (this.options, 'ws', this.urls['api']['ws']);
        const messageHash = 'ticker:' + symbol;
        const subscribe: Dict = {
            'event': 'subscribe',
            'channel': 'ticker',
            'symbol': symbol,
        };
        const request = this.deepExtend (subscribe, params);
        return await this.watch (url, messageHash, request, messageHash);
    }

    override async watchTickers (symbols: Strings = undefined, params = {}): Promise<Tickers> {
        await this.loadMarkets ();
        symbols = this.marketSymbols (symbols, undefined, false);
        const url = this.safeString (this.options, 'ws', this.urls['api']['ws']);
        const messageHashes = [];
        for (let i = 0; i < symbols.length; i++) {
            const symbol = symbols[i];
            const messageHash = 'ticker:' + symbol;
            messageHashes.push (messageHash);
            const subscribe: Dict = {
                'event': 'subscribe',
                'channel': 'ticker',
                'symbol': symbol,
            };
            const request = this.deepExtend (subscribe, params);
            this.watch (url, messageHash, request, messageHash);
        }
        return await this.watchMultiple (url, messageHashes, undefined, messageHashes);
    }

    override async watchTrades (symbol: string, since: Int = undefined, limit: Int = undefined, params = {}): Promise<Trade[]> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const url = this.safeString (this.options, 'ws', this.urls['api']['ws']);
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

    override async watchOrderBook (symbol: string, limit: Int = undefined, params = {}): Promise<OrderBook> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const url = this.safeString (this.options, 'ws', this.urls['api']['ws']);
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

    override handleMessage (client: Client, message: any) {
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
        if (event === 'ping' || message === 'ping') {
            client.send ({ 'event': 'pong' });
            return;
        }
        if (event === 'subscribed' || event === 'pong') {
            return;
        }
        if (event === 'error') {
            this.handleErrorMessage (client, message);
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

    handleErrorMessage (client: Client, message: any): boolean {
        const error = this.safeString2 (message, 'message', 'error');
        if (error !== undefined) {
            const feedback = this.id + ' ' + this.json (message);
            try {
                this.throwExactlyMatchedException (this.exceptions['exact'], error, feedback);
                this.throwBroadlyMatchedException (this.exceptions['broad'], error, feedback);
                throw new ExchangeError (feedback);
            } catch (e) {
                client.reject (e);
            }
            return true;
        }
        return false;
    }

    handleTicker (client: Client, message: any) {
        const symbol = this.safeString (message, 'symbol');
        if (symbol === undefined) {
            return;
        }
        const market = this.market (symbol);
        const data = this.safeDict (message, 'data', message);
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

    handleOrderBook (client: Client, message: any) {
        const symbol = this.safeString (message, 'symbol');
        if (symbol === undefined) {
            return;
        }
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

    handleTrades (client: Client, message: any) {
        const symbol = this.safeString (message, 'symbol');
        if (symbol === undefined) {
            return;
        }
        const market = this.market (symbol);
        let data = this.safeValue (message, 'data', []);
        if (data && !Array.isArray (data)) {
            data = [ data ];
        }
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

    override parseTrade (trade: Dict, market: Market = undefined): Trade {
        const timestamp = this.safeInteger (trade, 'timestamp');
        const id = this.safeString (trade, 'id');
        const price = this.safeString (trade, 'price');
        const amount = this.safeString (trade, 'amount');
        const side = this.safeString (trade, 'side');
        const symbol = this.safeSymbol (undefined, market);
        return this.safeTrade ({
            'id': id,
            'info': trade,
            'timestamp': timestamp,
            'datetime': this.iso8601 (timestamp),
            'symbol': symbol,
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
