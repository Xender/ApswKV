from setuptools import setup

setup(
	name = "ApswKV",
	# version = '',
	description = "SQLite-based key-value data store, using APSW.",
	url = "https://github.com/Xender/ApswKV",

	classifiers = [
		'Development Status :: 3 - Alpha',

		'Intended Audience :: Developers',

		'Programming Language :: Python :: 3',
		'Programming Language :: Python :: 3.5',
	],

	keywords = 'sqlite apsw dict key-value',

	packages = ['apsw_kv']
)
