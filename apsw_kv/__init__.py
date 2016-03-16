import collections
import functools

from .table import Table


def _wrap_transactional(func):
	@functools.wraps(func)
	def transactional_wrapper(self, *a, **kw):
		with self.conn:
			return func(self, *a, **kw)

	return transactional_wrapper


class ApswKV(Table, collections.MutableMapping):
	def __init__(self, conn, table):
		super().__init__(conn, table)

		self.create(
			# This is a single string!
			# Names are chosen short to type less when querying by hand.
			'k primary key, '  # Key
			'v, '              # Value
			'ts current_timestamp',
			if_not_exists=True
		)

	# Essential methods for :collections.MutableMapping:

	def __len__(self):
		[[n]] = self.select('count(*)')
		return n

	def __iter__(self):
		return ( row for [row] in self.select('k') )

	def __getitem__(self, key):
		cur = self.select('v', where=('k=?', key))
		try:
			[item] = next( cur )
			return item
		except StopIteration:
			raise KeyError(key)

	def __setitem__(self, key, item):
		# self._update('v', item, where=('k=?', key))
		self.replace('k, v', key, item)

	def __delitem__(self, key):
		self.delete(where=('k=?', key))

	# Transactional wrappers for methods provided by :collections.MutableMapping:

	# XXX What about __eq__?
	pop        = _wrap_transactional( collections.MutableMapping.pop )
	popitem    = _wrap_transactional( collections.MutableMapping.popitem )
	# clear      = _wrap_transactional( collections.MutableMapping.clear )  # Redefined below.
	update     = _wrap_transactional( collections.MutableMapping.update )
	setdefault = _wrap_transactional( collections.MutableMapping.setdefault )

	# Performance improvoments over methods provided by :collections.MutableMapping:

	class _ItemsView(collections.ItemsView):
		def __iter__(self):
			return self._mapping.select('k, v')

	class _ValuesView(collections.ItemsView):
		def __contains__(self, value):
			# vals = list(self._mapping.select('v', where=('v=?', value), limit=1))
			# return bool(vals)

			# [[n]] = self._mapping.select('count(*)', where=('v=?', value), limit=1)
			# return bool(n)

			subq = self._mapping._select_q('v', where=('v=?', value))
			subquery, binds = self._mapping._select_q('v').where('v=?', value)
			[[n]] = self._mapping.query(
				'select exists ({})'.format(subq.sql),
				subq.binds
			)
			return bool(n)

		def __iter__(self):
			return ( row for [row] in self.select('v') )

	def items(self):
		return self._ItemsView(self)

	def values(self):
		return self._ValuesView(self)

	def clear(self):
		"D.clear() -> None.  Remove all items from D."
		self.delete(where=None)

	# Additional methods

	def keys_for_value(val):
		return ( row for [row] in self.select('k', where=('v=?', val)) )
		return ( row for [row] in self.select('k').where('v=?', val) )
