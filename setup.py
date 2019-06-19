from setuptools import setup

setup(name='babylon',
      version='0.1',
      description='Easily connects multiple heterogeneous Python nodes by using RabbitMQ',
      url='http://github.com/m4nh/babylon',
      author='Daniele De Gregorio',
      author_email='daniele.degregorio@eyecan.ai',
      license='MIT',
      packages=['babylon'],
      install_requires=[
          'pika',
      ],
      zip_safe=False
      )
