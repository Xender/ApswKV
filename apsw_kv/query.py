def _clause_appender(clause):
	def append_clause(self, expr, *binds):
		if expr:
			self.sql += ' {} {}'.format(clause, expr)
			self.binds.extend(binds)

		return self

	append_clause.__name__ = clause

	return append_clause


class Query:
	def __init__(self, sql, binds=()):
		self.sql   = sql
		self.binds = list(binds)

	def exe(self, conn):
		return conn.cursor().execute(self.sql, self.binds)

	__call__ = exe

	@classmethod
	def create(cls, table, ):

	# def where(self, expr, *binds):
	# 	self.sql += ' where ' + expr
	# 	self.binds.extend(binds)
	#
	# def order_by(self, expr, *binds):
	# 	self.sql += ' order_by ' + expr
	# 	self.binds.extend(binds)
	#
	# def limit(self, expr, *binds):
	# 	self.sql += ' limit ' + expr
	# 	self.binds.extend(binds)

	where    = _clause_appender('where')
	order_by = _clause_appender('order_by')
	limit    = _clause_appender('limit')
