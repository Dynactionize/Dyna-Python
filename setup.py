from __future__ import print_function
import setuptools

import os
import shutil
import subprocess
import sys

from distutils.command.build_py import build_py as _build_py
from distutils.command.clean import clean as _clean
from distutils.debug import DEBUG
from distutils.dist import Distribution
from distutils.spawn import find_executable
from setuptools import setup, find_packages

with open("README.md") as fh:
    long_description = fh.read()

CLEANUP_SUFFIXES = [
    '_pb2.py',
    '_pb2_grpc.py',
    '.so',
    '.o',
    'protobuf/compiler/__init__.py',
]

if 'PROTOC' in os.environ and os.path.exists(os.environ['PROTOC']):
    protoc = os.environ['PROTOC']
else:
    protoc = find_executable('protoc')


def generate_proto(dir):
    if not os.path.exists(dir):
        sys.stderr.write('Can\t find required protobuffer submodule: {0}\n'.format(dir))
        sys.exit(1)
    
    if protoc is None:
        sys.stderr.write('protoc not found. Is the protobuf compiler installed?\n')
        sys.exit(1)


    script = """
import os
import sys
from grpc_tools import protoc

proto_path = os.path.abspath(os.path.join(dir, 'proto'))
python_out = os.path.abspath('dynapython')
input_dir = os.path.abspath(os.path.join(dir, 'proto', 'dynagatewaytypes'))
files = [f for f in os.listdir(input_dir) if f.endswith('.proto')]
print(proto_path)
for f in files:
    full_file = os.path.join(input_dir, f)
    print(full_file)
    protoc_args = ['--proto_path={0}'.format(proto_path),
                   '-I{0}'.format(proto_path),
                   '--python_out={0}'.format(python_out),
                   '--grpc_python_out={0}'.format(python_out),
                   full_file]
    print(protoc_args)
    if protoc.main(protoc_args) != 0:
        sys.exit(1)

init_file = os.path.join(python_out, 'dynagatewaytypes', '__init__.py')

with open(init_file, 'w') as f:
    f.write("import sys\\n")
    f.write("import os\\n")
    f.write("sys.path.append(os.path.dirname(os.path.dirname(__file__)))\\n")

    """

    exec(script, {
        'dir': dir,
    })


#    if subprocess.call(protoc_command) != 0:
#        sys.exit(1)


class DynaDistribution(Distribution):
    def __init__(self, attrs=None):
        self.protodirs = []
        self.cleandirectories = []
        self.cleansuffixes = ['_pb2.py', '_pb2_grc.py', '.pyc']
        cmdclass = attrs.get('cmdclass')
        if not cmdclass:
            cmdclass = {}

        if 'build_py' not in cmdclass:
            cmdclass['build_py'] = DynaBuildPy
        if 'clean' not in cmdclass:
            cmdclass['clean'] = DynaClean

        attrs['cmdclass'] = cmdclass
        Distribution.__init__(self, attrs)


class DynaClean(_clean):
    def run(self):
        try:
            cleandirectories = self.distribution.cleandirectories
        except AttributeError:
            sys.stderr.write('Error: cleandirectories not defined. DynaDistribution not used?')
            sys.exit(1)

        try:
            cleansuffixes = self.distribution.cleansuffixes
        except AttributeError:
            sys.stderr.write('Error: cleansuffixed not defined. DynaDistribution not used?')
            sys.exit(1)

        for directory in cleandirectories:
            if os.path.exists(directory):
                if DEBUG:
                    print('Removing directory "{0}"'.format(directory))
                shutil.rmtree(directory)

        for dirpath, _, filenames in os.walk('.'):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                for i in cleansuffixes:
                    if filepath.endswith(i):
                        if DEBUG:
                            print('Removing file: "{0}"'.format(filepath))
                        os.remove(filepath)

        _clean.run(self)


class DynaBuildPy(_build_py):
    def run(self):
        try:
            protodirs = self.distribution.protodirs
        except AttributeError:
            sys.stderr.write('Error: protodirs not defined. DynaDistribution not used?')
            sys.exit(1)

        for dir in protodirs:
            generate_proto(dir)

        _build_py.run(self)

setup(
    distclass=DynaDistribution,
    name="dyna-python-dynactionize.com",
    version="1.0.1",
    author="Dynactionize NV",
    author_email="info-belgium@dynationize.com",
    description="Python connector for the Dynizer",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Dynactionize/Dyna-Python",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    setup_requires=[
        "protobuf",
        "grpcio",
        "grpcio-tools",
    ],
    install_requires=[
        "protobuf",
        "grpcio",
    ],
    python_requires="~=3.6",
    protodirs=[
        "dynagatewaytypes"
    ],
    cleansuffixes=CLEANUP_SUFFIXES,
    cleandirectories=[],
)