import functools

def _remove_suffix(s, suffix):
	if s.endswith(suffix):
		return s[:-len(s)]

	return s


def _query_func(sql_func):
	@functools.wraps(sql_func)
	def exec_query(self, *a, **kw):
		return return self.exec(
			*sql_func(self, *a, **kw)
		)

	suffix = '_sql'
	exec_query.__name__     = _remove_suffix(sql_func.__name__,     suffix)
	exec_query.__qualname__ = _remove_suffix(sql_func.__qualname__, suffix)

	return exec_query


class Table:
	def __init__(self, conn, table):
		self.conn = conn
		self.table = table

	def exec(sql, binds=()):
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

	# def _append_limit(self, sql, binds, limit_arg):
	# 	expr, clause_binds = _parse_clause_arg(limit_arg)
	#
	# 	sql += ' limit ' + str(expr)
	# 	binds = *binds, *clause_binds
	#
	# 	return sql, binds

	# Queries SQL

	def _create_sql(self, fields, *, if_not_exists=True):
		sql = (
			# Short names are chosen to type less when querying by hand.
			'create table {if_not_exists}"{0.table}" '
			'( {1} )'
		).format(
			self,
			fields,
			if_not_exists = ('if not exists ' if if_not_exists else '')
		)

		return (sql,)

	def _drop_sql(self, *, if_not_exists=True):
		sql = (
			'drop table {if_not_exists}"{0.table}"'
		).format(
			self,
			if_not_exists = ('if not exists ' if if_not_exists else '')
		)

		return (sql,)

	def _select_sql(self, what, where=None):
		sql = 'select {1} from "{0.table}"'.format(self, what)

		if where:
			return _append_where(sql, (), where)

		return sql, ()

	def _replace_sql(self, what, *binds):
		sql = 'replace into "{0.table}" {1} values ({bind_markers})'.format(
			self,
			what,
			bind_markers = ','.join( '?' * len(binds) )  # Ugh.
		)

		return sql, binds

	def _delete_sql(self, where):
		# where can be None, but it has to be explicit for this operation!
		sql = 'delete from "{0.table}"'.format(self)

		if where:
			return _append_where(sql, (), where)

		return sql, ()

	# Queries execution

	create  = _query_func(_create_sql)
	drop    = _query_func(_drop_sql)
	select  = _query_func(_select_sql)
	replace = _query_func(_replace_sql)
	delete  = _query_func(_delete_sql)

	# def create(self):
	# 	return self.exec(
	# 		*self._create_sql()
	# 	)
	#
	# def select(self, what, where=None):
	# 	return self.exec(
	# 		*self._select_sql(what, where)
	# 	)
	#
	# def replace(self, what, *binds):
	# 	return self.exec(
	# 		*self._replace_sql(what, *binds)
	# 	)
	#
	# def delete(self, where):
	# 	# where can be None, but it has to be explicit for this operation!
	# 	return self.exec(
	# 		*self._delete_sql(where)
	# 	)
