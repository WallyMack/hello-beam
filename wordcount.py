# -*- coding: utf-8 -*-

from __future__ import print_function
import re
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


options = PipelineOptions()
with beam.Pipeline(options=options) as p:
    lines = p | beam.io.ReadFromText('wordcount.txt')

    counts = (
        lines
        | 'Split' >> (beam.FlatMap(lambda x: re.split(r'\s+', x))
                      .with_output_types(unicode))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
    )

    counts | 'Print' >> beam.ParDo(lambda (w, c): print('%s: %s' % (w, c)))
