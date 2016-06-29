Embulk::JavaPlugin.register_output(
  "swift", "org.embulk.output.swift.SwiftFileOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
