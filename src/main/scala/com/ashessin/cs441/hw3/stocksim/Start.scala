package com.ashessin.cs441.hw3.stocksim

import java.io.{File, IOException}
import java.net.URISyntaxException
import java.nio.file.{Files, Paths}
import java.util
import java.util.Objects
import java.util.regex.Pattern

import com.typesafe.config.{ConfigException, ConfigFactory}
import org.apache.log4j.PropertyConfigurator.configure
import org.slf4j.LoggerFactory

/**
 * Projects main class.
 */
object Start {
  private val logger = LoggerFactory.getLogger(classOf[Start])

  def main(args: Array[String]): Unit = {
    configure(Thread.currentThread.getContextClassLoader.getResource("log4j.properties"))
    // org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.DEBUG)
    new Start(args)
  }
}

class Start(val args: Array[String]) {
  try if (!parseCommandLineOptions(args)) System.exit(1)
  else System.exit(0)
  catch {
    case e: ConfigException.Missing =>
      Start.logger.error("trying to load the input config file: ", e)
    case e: ConfigException.Parse =>
      Start.logger.error("trying to parse the input config file: ", e)
    case e: Exception =>
      Start.logger.error("An unexpected error happened: ", e)
  }

  @throws[Exception]
  private def parseCommandLineOptions(args: Array[String]): Boolean = {
    if (args.length < 1) {
      System.out.println(showHelp)
      return false
    }
    if (args(0).trim.equalsIgnoreCase("--configFile")) {
      if (args.length == 2) {
        val configFile = new File(args(1))
        Start.logger.info("Trying to load config file {}", configFile.getAbsolutePath)
        executeConfig(configFile)
      }
      else {
        Start.logger.info("Executing default config")
        val confgFile = new File(Objects.requireNonNull(
          Thread.currentThread.getContextClassLoader.getResource("reference.conf")).toURI)
        executeConfig(confgFile)
      }
      return true
    }

    val className = args(0).trim + "$"
    val argsNew = util.Arrays.copyOfRange(args, 1, args.length)

    if (className.equalsIgnoreCase("--simulate") ||
      className == RunMontecarloSimulation.getClass.getName ||
      className == RunMontecarloSimulation.getClass.getSimpleName) {
      RunMontecarloSimulation.main(argsNew)
      return true
    }
    false
  }

  @throws[Exception]
  private def executeConfig(configSource: File): Unit = {
    if (configSource == null || "" == configSource.getAbsolutePath)
      throw new IllegalArgumentException("You must specify a file as command line parameter to --configFile.")
    val config = ConfigFactory.load(configSource.getPath)
    val jobs = config.getConfigList("jobs")
    import scala.collection.JavaConversions._
    for (job <- jobs) {
      val arguments = new util.ArrayList[String](3)
      arguments.add(job.getString("class"))
      arguments.addAll(job.getStringList("args"))
      parseCommandLineOptions(arguments.toArray(new Array[String](0)))
    }
  }

  @throws[IOException]
  @throws[URISyntaxException]
  private def showHelp =
    new String(Files.readAllBytes(Paths.get(
      Thread.currentThread.getContextClassLoader.getResource("help.txt").toURI)))
      .replaceAll(classOf[Start].getName, getApplicationStartCmd)

  /**
   * Gets the command used to launch the application.
   * <p>
   * If the application was launched from the jar file, returns a command like "java -jar name-of-the-jar-file".
   * If it was launched directly from the class file, returns a command like "java class-file".
   *
   * @return
   */
  private def getApplicationStartCmd = {
    val fullClassFilePath = getFullClassFilePath
    val jarFile = regexMatch(fullClassFilePath)
    if (jarFile.isEmpty) "java " + classOf[Start].getName
    else "spark-submit " + jarFile
  }

  private def regexMatch(text: String): String = {
    val matcher = Pattern.compile(".*\\/(.*\\.jar).*\\/").matcher(text)
    if (matcher.find) return matcher.group(1)
    ""
  }

  /**
   * Gets the full file path of this class.
   * This may include the path of a jar if the class is being run from a jar package.
   *
   * @return
   */
  private def getFullClassFilePath = {
    val classFileName = '/' + classOf[Start].getName.replace('.', '/') + ".class"
    classOf[Start].getResource(classFileName).getFile
  }

  // TODO: Improve config file parsing and cli options
}