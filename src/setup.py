from distutils.core import setup

setup(
    name='ptp-scraper',
    version='0.1',
    packages=['com', 'com.dipack', 'com.dipack.scraper'],
    package_dir={'': 'src'},
    url='',
    license='',
    author='Dipack',
    author_email='',
    description='Simple scraper to get _all_ challans from the Pune Traffic Police website', requires=['requests',
                                                                                                       'validators',
                                                                                                       'bs4',
                                                                                                       'robobrowser',
                                                                                                       'pandas',
                                                                                                       'numpy']
)
