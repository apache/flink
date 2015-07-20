angular.module('flinkApp')

.filter "amDurationFormatExtended", (angularMomentConfig) ->
  amDurationFormatExtendedFilter = (value, format, durationFormat) ->
    return ""  if typeof value is "undefined" or value is null

    moment.duration(value, format).format(durationFormat, { trim: false })

  amDurationFormatExtendedFilter.$stateful = angularMomentConfig.statefulFilters

  amDurationFormatExtendedFilter