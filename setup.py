import setuptools

requires = [
    'katcp==0.6.2',
    'katpoint==0.7',
    'katportalclient==0.2.1',
    'numpy==1.16.4',
    'pyyaml==5.1.1',
    'redis==2.10.6',
    'tornado==4.5.3',
    ]

setuptools.setup(
    name="meerkat-backend-interface",
    version="1.0.0",
    url="https://github.com/UCBerkeleySETI/meerkat-backend-interface",
    license='MIT',

    author="Eric Michaud, Daniel Czech",
    author_email="ericjmichaud@berkeley.edu, danielc@berkeley.edu",

    description="Breakthrough Listen's interface to MeerKAT",
    long_description=open("README.md").read(),

    py_modules=[
        'coordinator',
        'katcp_start',
        'katportal_start',
        ],
    packages=setuptools.find_packages(),

    install_requires=requires,

    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],

    entry_points={
        'console_scripts': [
            'coordinator = coordinator:cli',
            'katcp_start = katcp_start:cli',
            'katportal_start = katportal_start:cli',
            ]
        },
    )
