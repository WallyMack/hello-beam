# -*- coding: utf-8 -*-

from __future__ import print_function
import re
import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class PrintFn(beam.DoFn):
    def process(self, element):
        print(element)


class Print(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.ParDo(print)


options = PipelineOptions()
with beam.Pipeline(options=options) as p:
    lines = p | 'Create' >> beam.io.ReadFromText('access.log')
    lines | beam.CombineGlobally(beam.combiners.CountCombineFn()) | beam.ParDo(PrintFn())
    lines | beam.combiners.Count.Globally() | Print()
