from setuptools import setup


setup(
    name='aiobean',
    version='0.1.0',
    description="Asyncio based beanstalkd client",
    author="Han Liang",
    author_email='blurrcat@gmail.com',
    url='https://github.com/blurrcat/aiobeanstalk',
    packages=[
        'aiobean',
    ],
    package_dir={'aiobean': 'aiobean'},
    include_package_data=True,
    install_requires=[],
    license="BSD license",
    zip_safe=False,
    keywords='aiobean',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
