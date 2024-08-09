import apache_beam as beam

# from apache_beam.io import ReadAllFromGCS
from apache_beam.io.fileio import MatchFiles, ReadMatches


class ReadSource(beam.PTransform):
    def __init__(self, source):
        self.source = source

    def expand(self, pcoll):
        # return pcoll | "Read Excel Files" >> ReadAllFromGCS(self.source + "/*.xls*")
        return (
            pcoll
            | "Match Excel Files" >> MatchFiles(self.source + "/*.xls*")
            | "Read Excel Files" >> ReadMatches()
        )
