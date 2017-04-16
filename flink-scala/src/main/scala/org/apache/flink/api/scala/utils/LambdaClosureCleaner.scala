package org.apache.flink.api.scala.utils

import java.lang.reflect.Method

import org.apache.flink.api.scala.{ClosureCleaner, LambdaReturnStatementFinder}
import org.slf4j.LoggerFactory

 object LambdaClosureCleaner {

  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }

  val LOG = LoggerFactory.getLogger(this.getClass)

  def clean(closure: AnyRef): Unit = {
    val writeReplaceMethod: Method = try {
      closure.getClass.getDeclaredMethod("writeReplace")
    } catch {
      case e: java.lang.NoSuchMethodException =>
        LOG.warn("Expected a Java lambda; got " + closure.getClass.getName)
        return
    }

    writeReplaceMethod.setAccessible(true)
    // Because we still need to support Java 7, we must use reflection here.
    val serializedLambda: AnyRef = writeReplaceMethod.invoke(closure)
    if (serializedLambda.getClass.getName != "java.lang.invoke.SerializedLambda") {
      LOG.warn("Closure's writeReplace() method " +
        s"returned ${serializedLambda.getClass.getName}, not SerializedLambda")
      return
    }

    val serializedLambdaClass = classForName("java.lang.invoke.SerializedLambda")

    val implClassName = serializedLambdaClass
      .getDeclaredMethod("getImplClass").invoke(serializedLambda).asInstanceOf[String]
    // TODO: we do not want to unconditionally strip this suffix.
    val implMethodName = {
      serializedLambdaClass
        .getDeclaredMethod("getImplMethodName").invoke(serializedLambda).asInstanceOf[String]
        .stripSuffix("$adapted")
    }
    val implMethodSignature = serializedLambdaClass
      .getDeclaredMethod("getImplMethodSignature").invoke(serializedLambda).asInstanceOf[String]
    val capturedArgCount = serializedLambdaClass
      .getDeclaredMethod("getCapturedArgCount").invoke(serializedLambda).asInstanceOf[Int]
    val capturedArgs = (0 until capturedArgCount).map { argNum: Int =>
      serializedLambdaClass
        .getDeclaredMethod("getCapturedArg", java.lang.Integer.TYPE)
        .invoke(serializedLambda, argNum.asInstanceOf[Object])
    }
    assert(capturedArgs.size == capturedArgCount)
    val implClass = classForName(implClassName.replaceAllLiterally("/", "."))

    // Fail fast if we detect return statements in closures.
    // TODO: match the impl method based on its type signature as well, not just its name.
    ClosureCleaner
      .getClassReader(implClass)
      .accept(new LambdaReturnStatementFinder(implMethodName), 0)

    // Check serializable TODO: add flag
    ClosureCleaner.ensureSerializable(closure)
    capturedArgs.foreach(ClosureCleaner.clean(_))

    // TODO: null fields to render the closure serializable?
  }
}

