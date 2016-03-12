import collections
import functools


def _wrap_transactional(func):
	@functools.wraps(func)
	def transaction_wrapper(self, *a, **kw):
		with self.conn:
			return func(self, *a, **kw)

	return transaction_wrapper



class _Queryable:
	def _exec(sql, binds=()):
		return self.conn.cursor().execute(sql, binds)

	# SQL synthesis

	def _parse_clause_arg(clause_arg):
		if isinstance(clause_arg, (tuple, list)):
			expr, *clause_binds = clause_arg
			# binds = *binds, *clause_binds
		else:  # A string
			expr = clause_arg
			clause_binds = ()

		return expr, clause_binds

	def _append_where(self, sql, binds, where_arg):
		expr, clause_binds = _parse_clause_arg(where_arg)

		sql += ' where ' + expr
		binds = *binds, *clause_binds

		return sql, binds

	def _append_limit(self, sql, binds, limit_arg):
		expr, clause_binds = _parse_clause_arg(limit_arg)

		sql += ' limit ' + str(expr)
		binds = *binds, *clause_binds

		return sql, binds

	def _select_sql(self, what, where=None, limit=None):
		sql = 'select {1} from "{0.table}"'.format(self, what)

		if where:
			return _append_where(sql, (), where)

		return sql, ()

	def _replace_sql(self, what, *binds):
		sql = 'replace into "{0.table}" {1} values ({bind_markers})'.format(
			self,
			what,
			bind_markers=','.join( '?' * len(binds) )  # Ugh.
		)

		return sql, binds

	def _delete_sql(self, where):
		# where can be None, but it has to be explicit for this operation!
		sql = 'delete from "{0.table}"'.format(self)

		if where:
			return _append_where(sql, (), where)

		return sql, ()

	# Concrete queries

	def _select(self, what, where=None, limit=None):
		return self._exec(
			*self._select_sql(what, where)
		)

	def _replace(self, what, *binds):
		return self._exec(
			*self._replace_sql(what, *binds)
		)

	def _delete(self, where):
		# where can be None, but it has to be explicit for this operation!
		return self._exec(
			*self._delete_sql(where)
		)



class ApswKV(_Queryable, collections.MutableMapping):
	_create_sql_t = (
		# Short names are chosen to type less when querying by hand.
		'CREATE TABLE IF NOT EXISTS "{0.table}" '
		'( '
		'  k PRIMARY KEY, '  # Key
		'  v, '  # Value
		'  ts'  # Timestamp
		')'
	)
	# _get_sql_t = 'SELECT v FROM "{0.table}" WHERE k = ?'
	# _add_sql_t = 'INSERT INTO "{0.table}" (k, v, ts) VALUES (?, ?, ?)'
	# _set_sql_t = 'REPLACE INTO "{0.table}" (k, v, ts) VALUES (?, ?, ?)'
	# _del_sql_t = 'DELETE FROM "{0.table}" WHERE k = ?'

	def __init__(self, conn, table):
		self.conn = conn
		self.table = table

		self._create_sql = self._create_sql_t.format(self)
		# self._get_sql    = self._get_sql_t.format(self)
		# self._add_sql    = self._add_sql_t.format(self)
		# self._set_sql    = self._set_sql_t.format(self)
		# self._del_sql    = self._del_sql_t.format(self)

		# with conn:
		conn.cursor().execute(self._create_sql)

	# Essential methods for :collections.MutableMapping:

	def __len__(self):
		[[n]] = self._select('count(*)')
		return n

	def __iter__(self):
		return ( row for [row] in self._select('k') )

	def __getitem__(self, key):
		cur = self._select('v', where=('k=?', key))
		try:
			[item] = next( cur )
			return item
		except StopIteration:
			raise KeyError(key)

	def __setitem__(self, key, item):
		# self._update('v', item, where=('k=?', key))
		self._replace('k, v', key, item)

	def __delitem__(self, key):
		self._delete(where=('k=?', key))

	# Transactional wrappers for methods provided by :collections.MutableMapping:

	# XXX What about __eq__?
	pop        = _wrap_transactional( collections.MutableMapping.pop )
	popitem    = _wrap_transactional( collections.MutableMapping.popitem )
	clear      = _wrap_transactional( collections.MutableMapping.clear )
	update     = _wrap_transactional( collections.MutableMapping.update )
	setdefault = _wrap_transactional( collections.MutableMapping.setdefault )

	# Performance improvoments over methods provided by :collections.MutableMapping:

	class ItemsView(collections.ItemsView):
		def __iter__(self):
			return self._mapping._select('k, v')

	class ValuesView(collections.ItemsView):
		def __contains__(self, value):
			[[n]] = self._mapping._select('count(*)', where=('v=?', value), limit=1)
			return len(n)

		def __iter__(self):
			return ( row for [row] in self._select('v') )

	def items(self):
		return ApswKV.ItemsView(self)

	def values(self):
		return ApswKV.ValuesView(self)

	def clear(self):
		"D.clear() -> None.  Remove all items from D."
		self._delete(where=None)
