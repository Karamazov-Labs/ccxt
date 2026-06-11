//  ---------------------------------------------------------------------------

import Exchange from './abstract/kinesis.js';
import { ExchangeError, AuthenticationError, ArgumentsRequired, BadRequest, InsufficientFunds, RateLimitExceeded } from './base/errors.js';
import { TICK_SIZE } from './base/functions/number.js';
import { sha256 } from './static_dependencies/noble-hashes/sha256.js';
import type { Balances, Dict, Int, Market, Num, Order, OrderBook, OrderSide, OrderType, Str, Ticker, OHLCV, LedgerEntry, Currency, Transaction, TransferEntry } from './base/types.js';

//  ---------------------------------------------------------------------------

/**
 * @class kinesis
 * @augments Exchange
 */
export default class kinesis extends Exchange {
    describe (): any {
        return this.deepExtend (super.describe (), {
            'id': 'kinesis',
            'name': 'Kinesis',
            'countries': [ 'UK' ],
            'rateLimit': 1000,
            'version': 'v1',
            'has': {
                'CORS': undefined,
                'spot': true,
                'margin': false,
                'swap': false,
                'future': false,
                'option': false,
                'cancelOrder': true,
                'createOrder': true,
                'fetchBalance': true,
                'fetchLedger': true,
                'fetchMarkets': true,
                'fetchOHLCV': true,
                'fetchOpenOrders': true,
                'fetchOrder': true,
                'fetchOrderBook': true,
                'fetchTicker': true,
                'transfer': true,
                'withdraw': true,
            },
            'urls': {
                'logo': 'https://user-images.githubusercontent.com/1294454/27891111-02b9e69c-61d4-11e7-8b0c-4fa81781b0a8.png',
                'api': {
                    'rest': 'https://api.kinesis.money',
                },
                'www': 'https://kinesis.money',
                'doc': [
                    'https://github.com/bullioncapital/kinesis-api',
                ],
            },
            'fees': {
                'trading': {
                    'tierBased': false,
                    'percentage': true,
                    'taker': this.parseNumber ('0.0022'),
                    'maker': this.parseNumber ('0.0022'),
                },
            },
            'api': {
                'private': {
                    'get': [
                        'exchange/pairs',
                        'exchange/depth/{pair}',
                        'exchange/ohlc/{pair}',
                        'exchange/mid-price/{pair}',
                        'exchange/holdings',
                        'exchange/orders/open',
                        'exchange/orders/{id}',
                        'exchange/reporting/account-balance-statement',
                        'exchange/reporting/account-balance-statement/{currency}',
                    ],
                    'post': [
                        'exchange/orders',
                        'exchange/withdrawals/address',
                        'exchange/withdrawals/email',
                    ],
                    'delete': [
                        'exchange/orders/{id}',
                    ],
                },
            },
            'timeframes': {
                '1m': '1',
                '5m': '5',
                '15m': '15',
                '30m': '30',
                '1h': '60',
                '4h': '240',
                '1d': '1440',
            },
            'precisionMode': TICK_SIZE,
            'features': {
                'spot': {
                    'sandbox': false,
                    'createOrder': {
                        'marginMode': false,
                        'triggerPrice': false,
                        'triggerDirection': false,
                        'triggerPriceType': undefined,
                        'stopLossPrice': false,
                        'takeProfitPrice': false,
                        'attachedStopLossTakeProfit': undefined,
                        'timeInForce': {
                            'IOC': false,
                            'FOK': false,
                            'PO': false,
                            'GTD': false,
                        },
                        'hedged': false,
                        'trailing': false,
                        'leverage': false,
                        'marketBuyByCost': false,
                        'marketBuyRequiresPrice': false,
                        'selfTradePrevention': false,
                        'iceberg': false,
                    },
                    'createOrders': undefined,
                    'fetchMyTrades': undefined,
                    'fetchOrder': {
                        'marginMode': false,
                        'trigger': false,
                        'trailing': false,
                        'symbolRequired': false,
                    },
                    'fetchOpenOrders': {
                        'marginMode': false,
                        'limit': undefined,
                        'trigger': false,
                        'trailing': false,
                        'symbolRequired': false,
                    },
                    'fetchOrders': undefined,
                    'fetchClosedOrders': undefined,
                    'fetchOHLCV': {
                        'limit': undefined,
                    },
                },
            },
            'exceptions': {
                'exact': {
                    'Forbidden': AuthenticationError,
                    'Forbidden resource': AuthenticationError,
                    'In sufficient funds': InsufficientFunds,
                    'Not authorised': AuthenticationError,
                },
                'broad': {
                    'Failed to reserve balance': InsufficientFunds,
                    'Rate Limit Exceeded': RateLimitExceeded,
                    'Too Many Requests': RateLimitExceeded,
                },
            },
        });
    }

    async fetchMarkets (params = {}): Promise<Market[]> {
        const response = await this.privateGetExchangePairs (params);
        const result = [];
        for (let i = 0; i < response.length; i++) {
            const market = response[i];
            const id = this.safeString (market, 'currencyPairId');
            const baseId = this.safeString (market, 'baseCurrency');
            const quoteId = this.safeString (market, 'quoteCurrency');
            const base = this.safeCurrencyCode (baseId);
            const quote = this.safeCurrencyCode (quoteId);
            const symbol = base + '/' + quote;
            const pricePrecision = this.safeString (market, 'pricePrecision', '4');
            const amountPrecision = this.safeString (market, 'amountPrecision', '2');
            const price = this.parseNumber (this.parsePrecision (pricePrecision));
            const amount = this.parseNumber (this.parsePrecision (amountPrecision));
            const minAmount = this.safeNumber (market, 'minAmount');
            result.push ({
                'id': id,
                'symbol': symbol,
                'base': base,
                'quote': quote,
                'settle': undefined,
                'baseId': baseId,
                'quoteId': quoteId,
                'settleId': undefined,
                'type': 'spot',
                'spot': true,
                'margin': false,
                'swap': false,
                'future': false,
                'option': false,
                'active': true,
                'contract': false,
                'linear': undefined,
                'inverse': undefined,
                'contractSize': undefined,
                'expiry': undefined,
                'expiryDatetime': undefined,
                'strike': undefined,
                'optionType': undefined,
                'precision': {
                    'amount': amount,
                    'price': price,
                },
                'limits': {
                    'leverage': {
                        'min': undefined,
                        'max': undefined,
                    },
                    'amount': {
                        'min': minAmount,
                        'max': undefined,
                    },
                    'price': {
                        'min': undefined,
                        'max': undefined,
                    },
                    'cost': {
                        'min': undefined,
                        'max': undefined,
                    },
                },
                'info': market,
            });
        }
        return result;
    }

    async fetchTicker (symbol: string, params = {}): Promise<Ticker> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        const request = {
            'pair': market['id'],
        };
        const response = await this.privateGetExchangeMidPricePair (this.extend (request, params));
        return this.parseTicker (response, market);
    }

    parseTicker (ticker: Dict, market: Market = undefined): Ticker {
        const symbol = this.safeSymbol (undefined, market);
        const last = this.safeString (ticker, 'mid_price');
        const timestamp = this.milliseconds ();
        return this.safeTicker ({
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': this.iso8601 (timestamp),
            'high': undefined,
            'low': undefined,
            'bid': this.safeString (ticker, 'bid'),
            'bidVolume': undefined,
            'ask': this.safeString (ticker, 'ask'),
            'askVolume': undefined,
            'vwap': undefined,
            'open': undefined,
            'close': last,
            'last': last,
            'previousClose': undefined,
            'change': undefined,
            'percentage': undefined,
            'average': undefined,
            'baseVolume': undefined,
            'quoteVolume': undefined,
            'info': ticker,
        }, market);
    }

    async fetchOrderBook (symbol: string, limit: Int = undefined, params = {}): Promise<OrderBook> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        const request = {
            'pair': market['id'],
        };
        const response = await this.privateGetExchangeDepthPair (this.extend (request, params));
        const depthItems = this.safeDict (response, 'depthItems', {});
        return this.parseOrderBook (depthItems, market['symbol'], undefined, 'bid', 'ask', 'price', 'amount');
    }

    async fetchOHLCV (symbol: string, timeframe: string = '1m', since: Int = undefined, limit: Int = undefined, params = {}): Promise<OHLCV[]> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        const request = {
            'pair': market['id'],
        };
        const timeframeValue = this.safeString (this.timeframes, timeframe);
        if (timeframeValue === undefined) {
            throw new BadRequest (this.id + ' fetchOHLCV does not support timeframe ' + timeframe);
        }
        request['timeFrame'] = timeframeValue;
        if (since !== undefined) {
            request['fromDate'] = this.iso8601 (since);
        } else {
            request['fromDate'] = this.iso8601 (this.milliseconds () - 30 * 24 * 60 * 60 * 1000);
        }
        request['toDate'] = this.iso8601 (this.milliseconds ());
        const response = await this.privateGetExchangeOhlcPair (this.extend (request, params));
        return this.parseOHLCVs (response, market, timeframe, since, limit);
    }

    parseOHLCV (ohlcv, market: Market = undefined): OHLCV {
        const datetime = this.safeStringN (ohlcv, [ 'time', 'timestamp', 'date', 'at', 'dateTime', 'datetime' ]);
        const timestamp = this.parse8601 (datetime);
        return [
            timestamp,
            this.safeNumberN (ohlcv, [ 'open', 'o' ]),
            this.safeNumberN (ohlcv, [ 'high', 'h' ]),
            this.safeNumberN (ohlcv, [ 'low', 'l' ]),
            this.safeNumberN (ohlcv, [ 'close', 'c' ]),
            this.safeNumberN (ohlcv, [ 'volume', 'v' ]),
        ];
    }

    async fetchBalance (params = {}): Promise<Balances> {
        await this.loadMarkets ();
        const response = await this.privateGetExchangeHoldings (params);
        return this.parseBalance (response);
    }

    parseBalance (response): Balances {
        const result: Dict = { 'info': response };
        const keys = Object.keys (response);
        for (let i = 0; i < keys.length; i++) {
            const currencyId = keys[i];
            const entry = response[currencyId];
            const code = this.safeCurrencyCode (currencyId);
            const account = this.account ();
            account['free'] = this.safeString (entry, 'available');
            account['used'] = this.safeString (entry, 'allocatedOnExchange');
            result[code] = account;
        }
        return this.safeBalance (result);
    }

    async fetchLedger (code: Str = undefined, since: Int = undefined, limit: Int = undefined, params = {}): Promise<LedgerEntry[]> {
        await this.loadMarkets ();
        let currency = undefined;
        const request: Dict = {};
        let response = undefined;
        if (code !== undefined) {
            currency = this.currency (code);
            request['currency'] = currency['id'];
            response = await this.privateGetExchangeReportingAccountBalanceStatementCurrency (this.extend (request, params));
        } else {
            response = await this.privateGetExchangeReportingAccountBalanceStatement (params);
        }
        return this.parseLedger (response, currency, since, limit);
    }

    parseLedgerEntry (item: Dict, currency: Currency = undefined): LedgerEntry {
        const id = this.safeString (item, 'Transaction_ID');
        const datetime = this.safeString (item, 'DateTime');
        const timestamp = this.parse8601 (datetime);
        const currencyId = this.safeString (item, 'Currency_Code');
        const code = this.safeCurrencyCode (currencyId, currency);
        currency = this.safeCurrency (currencyId, currency);
        const amount = this.safeNumber (item, 'Amount');
        const before = this.safeNumber (item, 'Starting_Balance');
        const after = this.safeNumber (item, 'Closing_Balance');
        const typeString = this.safeString (item, 'Transaction_Type');
        const type = this.parseLedgerEntryType (typeString);
        let fee = undefined;
        const feeCost = this.safeNumber (item, 'Fee');
        if (feeCost !== undefined) {
            const feeCurrencyId = this.safeString (item, 'Fee_Currency');
            const feeCurrencyCode = this.safeCurrencyCode (feeCurrencyId);
            fee = {
                'cost': feeCost,
                'currency': feeCurrencyCode,
            };
        }
        return this.safeLedgerEntry ({
            'info': item,
            'id': id,
            'timestamp': timestamp,
            'datetime': this.iso8601 (timestamp),
            'direction': undefined,
            'account': undefined,
            'referenceId': this.safeString (item, 'Order_ID'),
            'referenceAccount': undefined,
            'type': type,
            'currency': code,
            'amount': amount,
            'before': before,
            'after': after,
            'status': 'ok',
            'fee': fee,
        }, currency);
    }

    parseLedgerEntryType (type: Str): Str {
        if (type === undefined) {
            return undefined;
        }
        const downcased = type.toLowerCase ();
        if (downcased.indexOf ('trade') >= 0) {
            return 'trade';
        }
        if (downcased.indexOf ('deposit') >= 0) {
            return 'deposit';
        }
        if (downcased.indexOf ('withdraw') >= 0) {
            return 'withdrawal';
        }
        if (downcased.indexOf ('transfer') >= 0) {
            return 'transfer';
        }
        if (downcased.indexOf ('fee') >= 0) {
            return 'fee';
        }
        if (downcased.indexOf ('distribution') >= 0 || downcased.indexOf ('yield') >= 0 || downcased.indexOf ('interest') >= 0) {
            return 'interest';
        }
        return type;
    }

    async createOrder (symbol: string, type: OrderType, side: OrderSide, amount: number, price: Num = undefined, params = {}): Promise<Order> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        const request: Dict = {
            'currencyPairId': market['id'],
            'direction': side,
            'amount': amount,
            'orderType': type,
        };
        if (type === 'limit') {
            if (price === undefined) {
                throw new ArgumentsRequired (this.id + ' createOrder() requires a price argument for limit orders');
            }
            request['limitPrice'] = price;
        }
        const response = await this.privatePostExchangeOrders (this.extend (request, params));
        return this.parseOrder (response, market);
    }

    parseOrder (order: Dict, market: Market = undefined): Order {
        const id = this.safeString (order, 'id');
        const marketId = this.safeString (order, 'currencyPairId');
        const symbol = this.safeSymbol (marketId, market);
        const side = this.safeString (order, 'direction');
        const type = this.safeString (order, 'orderType');
        const price = this.safeString (order, 'limitPrice');
        const amount = this.safeString (order, 'amount');
        const remaining = this.safeString (order, 'remaining');
        const status = this.parseOrderStatus (this.safeString (order, 'status'));
        const datetime = this.safeString (order, 'dateTime');
        const timestamp = this.parse8601 (datetime);
        return this.safeOrder ({
            'info': order,
            'id': id,
            'clientOrderId': undefined,
            'timestamp': timestamp,
            'datetime': datetime,
            'lastTradeTimestamp': undefined,
            'symbol': symbol,
            'type': type,
            'timeInForce': undefined,
            'postOnly': undefined,
            'side': side,
            'price': price,
            'triggerPrice': undefined,
            'amount': amount,
            'cost': undefined,
            'average': undefined,
            'filled': undefined,
            'remaining': remaining,
            'status': status,
            'fee': undefined,
            'trades': undefined,
        }, market);
    }

    parseOrderStatus (status: Str): Str {
        const statuses: Dict = {
            'open': 'open',
            'filled': 'closed',
            'fill': 'closed',
            'cancelled': 'canceled',
            'rejected': 'rejected',
        };
        return this.safeString (statuses, status, status);
    }

    async cancelOrder (id: string, symbol: Str = undefined, params = {}): Promise<Order> {
        const request = {
            'id': id,
        };
        const response = await this.privateDeleteExchangeOrdersId (this.extend (request, params));
        return this.parseOrder (response);
    }

    async fetchOrder (id: string, symbol: Str = undefined, params = {}): Promise<Order> {
        await this.loadMarkets ();
        const request = {
            'id': id,
        };
        const response = await this.privateGetExchangeOrdersId (this.extend (request, params));
        return this.parseOrder (response);
    }

    async fetchOpenOrders (symbol: Str = undefined, since: Int = undefined, limit: Int = undefined, params = {}): Promise<Order[]> {
        await this.loadMarkets ();
        const request: Dict = {};
        let market = undefined;
        if (symbol !== undefined) {
            market = this.market (symbol);
            request['currencyPairId'] = market['id'];
        }
        const response = await this.privateGetExchangeOrdersOpen (this.extend (request, params));
        return this.parseOrders (response, market, since, limit);
    }

    async withdraw (code: string, amount: number, address: string, tag: Str = undefined, params = {}): Promise<Transaction> {
        await this.loadMarkets ();
        const currency = this.currency (code);
        const request = {
            'amount': amount,
            'currencyCode': currency['id'],
            'address': address,
        };
        if (tag !== undefined) {
            request['memo'] = tag;
        }
        const response = await this.privatePostExchangeWithdrawalsAddress (this.extend (request, params));
        return this.parseTransaction (response, currency);
    }

    parseTransaction (transaction: Dict, currency: Currency = undefined): Transaction {
        const id = this.safeString (transaction, 'id');
        const amount = this.safeNumber (transaction, 'amount');
        const address = this.safeString (transaction, 'address');
        const tag = this.safeString (transaction, 'memo');
        const datetime = this.safeString (transaction, 'dateTime');
        const timestamp = this.parse8601 (datetime);
        const currencyId = this.safeString (transaction, 'currencyCode');
        const code = this.safeCurrencyCode (currencyId, currency);
        return {
            'info': transaction,
            'id': id,
            'txid': undefined,
            'timestamp': timestamp,
            'datetime': datetime,
            'network': undefined,
            'address': address,
            'addressTo': address,
            'addressFrom': undefined,
            'tag': tag,
            'tagTo': tag,
            'tagFrom': undefined,
            'type': 'withdrawal',
            'amount': amount,
            'currency': code,
            'status': 'ok',
            'updated': undefined,
            'comment': undefined,
            'internal': undefined,
            'fee': undefined,
        };
    }

    async transfer (code: string, amount: number, fromAccount: string, toAccount: string, params = {}): Promise<TransferEntry> {
        await this.loadMarkets ();
        const currency = this.currency (code);
        const request = {
            'amount': amount,
            'currencyCode': currency['id'],
            'receiverEmail': toAccount,
        };
        const response = await this.privatePostExchangeWithdrawalsEmail (this.extend (request, params));
        return {
            'info': response,
            'id': this.safeString (response, 'id'),
            'timestamp': undefined,
            'datetime': undefined,
            'currency': code,
            'amount': amount,
            'fromAccount': fromAccount,
            'toAccount': toAccount,
            'status': 'ok',
        };
    }

    nonce (): number {
        return this.milliseconds ();
    }

    sign (path, api = 'public', method = 'GET', params = {}, headers = undefined, body = undefined) {
        let url = '/' + this.version + '/' + this.implodeParams (path, params);
        const query = this.omit (params, this.extractParams (path));
        this.checkRequiredCredentials ();
        const nonce = this.nonce ().toString ();
        let authMessage = nonce + method + url;
        if (method === 'POST') {
            if (Object.keys (query).length) {
                body = this.json (query);
                authMessage += body;
            }
        } else if (method === 'GET' || method === 'DELETE') {
            if (Object.keys (query).length) {
                url += '?' + this.urlencode (query);
            }
        }
        const signature = this.hmac (this.encode (authMessage), this.encode (this.secret), sha256).toUpperCase ();
        headers = {
            'x-nonce': nonce,
            'x-api-key': this.apiKey,
            'x-signature': signature,
        };
        if (method !== 'DELETE') {
            headers['Content-Type'] = 'application/json';
        }
        const finalUrl = this.urls['api']['rest'] + url;
        return { 'url': finalUrl, 'method': method, 'body': body, 'headers': headers };
    }

    handleErrors (httpCode: number, reason: string, url: string, method: string, headers: Dict, body: string, response, requestHeaders, requestBody) {
        if (response === undefined) {
            return undefined;
        }
        const message = this.safeString (response, 'message');
        if (message !== undefined) {
            const feedback = this.id + ' ' + body;
            this.throwExactlyMatchedException (this.exceptions['exact'], message, feedback);
            this.throwBroadlyMatchedException (this.exceptions['broad'], message, feedback);
            throw new ExchangeError (feedback);
        }
        return undefined;
    }
}
