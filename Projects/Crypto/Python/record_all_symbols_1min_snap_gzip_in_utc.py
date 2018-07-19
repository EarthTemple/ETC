import sys
sys.path.append(r'C:\ETC\Production\Codes\Python\Huobi\lib')
import HuobiServices as hs
import huo_parser as hp
import pandas as pd
import os 
import datetime as dt
import time 
from threading import Thread

class snapshot(Thread):
	reject_threshold = 1000

	def __init__(self, symbol, t0, t1, snap_rate_in_sec = 60, refresh_rate_in_sec = 1, convert_ts = 'utc', \
				 outfolder = r'C:\ETC\Production\Data\SnapshotsUTC\Huobi'):
		Thread.__init__(self)
		self.symbol = symbol
		self.t0 = t0
		self.t1 = t1
		self.snap_rate_in_sec = max(snap_rate_in_sec, 60) # do not support snaprate lower than 1min
		self.refresh_rate_in_sec = refresh_rate_in_sec
		self.convert_ts = convert_ts

		self.outfolder = os.path.join(outfolder, symbol)
		self.outfile = '{0}_snap_{1}.csv.gz'.format(symbol, t1.date())
		if not os.path.exists(self.outfolder):
			os.makedirs(self.outfolder)

		if os.path.exists(os.path.join(self.outfolder, self.outfile)):
			# print(os.path.join(self.outfolder, self.outfile))
			try:
				# print(self.outfile)
				self.df = pd.read_csv(os.path.join(self.outfolder, self.outfile), header = 0, index_col = 0, parse_dates = True)
			except OSError:
				self.df = pd.DataFrame(columns = ['open', 'high', 'low', 'close', 'volume', 'usd', 'trd_count', \
											  'bid_price', 'bid_qty', 'ask_price', 'ask_qty', \
											  'buy_price', 'buy_vol', 'sell_price', 'sell_vol'])	
		else:
			self.df = pd.DataFrame(columns = ['open', 'high', 'low', 'close', 'volume', 'usd', 'trd_count', \
											  'bid_price', 'bid_qty', 'ask_price', 'ask_qty', \
											  'buy_price', 'buy_vol', 'sell_price', 'sell_vol'])

	def run(self):
		# 1. get mkt top book info
		# 2. get bar (ohlc, vol, usd, trd_count)
		# 3. get order flow
		if dt.datetime.utcnow() > self.t1:
			return

		pre_ts = None
		logs = []
		reject_count = 0
		while 1:
			now = dt.datetime.utcnow()
			while now < self.t0:
				time.sleep(1)
				now = dt.datetime.utcnow()
			if now > self.t1:
				self.write(logs, 0)
				break

			if pre_ts is None or now - pre_ts >= dt.timedelta(seconds = self.snap_rate_in_sec):
				try:
					hdepth = hs.get_depth(self.symbol, 'step1')
					hkline = hs.get_kline(self.symbol, '1min', size = 1)
					htrade = hs.get_hist_trade(self.symbol, 2000)

					ts_data = hdepth['tick']['ts']
					ts_received = hdepth['ts']
					# latency = ts_received - ts_data
					latency = (hp._parse_ts(ts_received / 1000, 'local') - now).microseconds / 1000

					depth = hp.parse_depth(hdepth, level = 1, convert_ts = self.convert_ts)
					bar   = hp.parse_kline(hkline, convert_ts = self.convert_ts)
					trade = hp.parse_hist_trade(htrade, now - dt.timedelta(seconds = self.snap_rate_in_sec), now, convert_ts = self.convert_ts)

					info = list(bar.iloc[-1, :]) + list(depth) + list(trade.values())
					# self.df.loc[now] = info
					logs.append([now] + info)
					print(self.symbol, now, 'Latency: {0} ms'.format(latency))
					pre_ts = now
				except Exception as e:
					# print(e)
					if reject_count < self.reject_threshold:
						reject_count += 1
						print('{0} - {1} snapshot error, wait 10s and retry'.format(dt.datetime.utcnow(), self.symbol))
						if self.write(logs, 0):
							logs = []
						time.sleep(10)
						continue
					else:
						print('{0} - {1} breach reject threshold. killing thread.'.format(dt.datetime.utcnow(), self.symbol))
						self.write(logs, 0)
						break
			else:
				if self.write(logs):
					logs = []
			time.sleep(1)

	def write(self, logs, buffer_size = 5):
		if logs is None or len(logs) < buffer_size:
			return False
		new_df = pd.DataFrame(logs, columns = ['ts'] + list(self.df.columns))
		new_df.set_index('ts', inplace = True)

		self.df = pd.concat([self.df, new_df], axis = 0)
		self.df = self.df[~self.df.index.duplicated(keep = 'first')]
		self.df.to_csv(os.path.join(self.outfolder, self.outfile), compression = 'gzip')
		return True



# symbol = 'btcusdt'
# t0 = dt.datetime(2018,7,3,16,0)
# t1   = dt.datetime(2018,7,4,16,0)
# snap = snapshot(symbol, t0, t1)
# snap.run()

def get_symbols():
	# symbols = ['btcusdt', 'bchusdt', 'ethusdt', 'etcusdt', 'ltcusdt', 'eosusdt', 'adausdt', 'xrpusdt', 'omgusdt', 'iotausdt', 'steemusdt', 'dashusdt', 'zecusdt', 'hb10usdt']
	# symbols += ['bchbtc', 'ethbtc', 'ltcbtc', 'etcbtc', 'eosbtc', 'adabtc', 'xrpbtc', 'iotabtc', 'omgbtc', 'dashbtc', 'steembtc', 'zecbtc', 'xmrbtc']
	# symbols += ['eoseth', 'adaeth', 'omgeth', 'xmreth', 'steemeth', 'iotaeth']
	raw_data = hs.get_symbols()
	symbols = []
	for x in raw_data['data']:
		symbols.append(x['symbol'])
	return symbols
# print(get_symbols())

if __name__ == '__main__':
	symbols = get_symbols()
	now = dt.datetime.utcnow()
	today = now.date()
	if now.time() < dt.time(21,0):
		t1 = dt.datetime.combine(today, dt.time(21,0))
		t0 = t1 - dt.timedelta(days = 1)
	else:
		t0 = dt.datetime.combine(today, dt.time(21,0))
		t1 = t0 + dt.timedelta(days = 1)

	snaps = [snapshot(x, t0, t1) for x in symbols]
	# for sym in symbols:
	# 	snaps.append(snapshot(sym, t0, t1))
	for sp in snaps:
		sp.start()

	for sp in snaps:
		sp.join()

	print('done')