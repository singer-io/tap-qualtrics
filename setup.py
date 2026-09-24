

from setuptools import setup, find_packages


setup(name="tap-qualtrics",
      version="0.0.1",
      description="Singer.io tap for extracting data from Qualtrics API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_qualtrics"],
      install_requires=[
        "singer-python==6.8.0",
        "requests==2.34.2",
        "backoff==2.2.1",
      ],
      extras_require={
          'dev': [
              'parameterized'
          ]
      },
      entry_points="""
          [console_scripts]
          tap-qualtrics=tap_qualtrics:main
      """,
      packages=find_packages(),
      package_data = {
          "tap_qualtrics": ["schemas/*.json"],
      },
      include_package_data=True,
)
