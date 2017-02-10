package ru.lamoda.etl.hadoop

import ru.lamoda.etl.config.Config

/**
  * Created by gevorg.hachaturyan on 27/01/2017.
  */
class DataLoader(configParams: Config) {

  new MovingFiles().copyLocalToHDFS(configParams)

  new SparkExecute(configParams)

}
