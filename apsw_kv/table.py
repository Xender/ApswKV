import functools

from .query import Query


def _remove_suffix(s, suffix):
	if s.endswith(suffix):
		return s[:-len(s)]

	return s


def _query_func(sql_func):
	@functools.wraps(sql_func)
	def exec_query(self, *a, **kw):
		return sql_func(self, *a, **kw).exe(self.conn)

	suffix = '_q'
	exec_query.__name__     = _remove_suffix(sql_func.__name__,     suffix)
	exec_query.__qualname__ = _remove_suffix(sql_func.__qualname__, suffix)

	return exec_query


class Table:
	def __init__(self, conn, table):
		self.conn = conn
		self.table = table

	def query(sql, binds=()):
		return self.conn.cursor().execute(sql, binds)

	# Queries

	def create_q(self, fields, *, if_not_exists=False):
		sql = (
			# Short names are chosen to type less when querying by hand.
			'create table {if_not_exists} "{0.table}" '
			'( {1} )'
		).format(
			self, fields,
			if_not_exists = ('if not exists' if if_not_exists else '')
		)
		return Query(sql)

	def drop_q(self, *, if_exists=True):
		sql = (
			'drop table {if_exists}"{0.table}"'
		).format(
			self,
			if_exists = ('if exists ' if if_exists else '')
		)
		return Query(sql)

	def select_q(self, what, where=None, limit=None):
		sql = 'select {1} from "{0.table}"'.format(self, what)
		return Query(sql).where(where).limit(limit)

	def insert_q(self, what, *binds, on_conflict=None):
		sql = 'insert {or_} into "{0.table}" ({1}) values ({bind_markers})'.format(
			self, what,
			or_ = ( ('or ' + on_conflict) if on_conflict else '' ),
			bind_markers = ','.join( '?' * len(binds) )  # Ugh.
		)
		return Query(sql, binds)

	def replace_q(self, what, *binds):
		return self.insert_q(what, *binds, on_conflict='replace')

	def delete_q(self, where, limit=None):
		# where can be None, but it has to be explicit for this operation!
		sql = 'delete from "{0.table}"'.format(self)
		return Query(sql).where(where).limit(limit)

	# Queries execution

	create  = _query_func(create_q)
	drop    = _query_func(drop_q)

	select  = _query_func(select_q)
	insert  = _query_func(insert_q)
	replace = _query_func(replace_q)
	delete  = _query_func(delete_q)
