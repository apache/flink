/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.api.scala.operators

import java.lang.reflect.Field

import scala.collection.mutable.Map
import scala.collection.mutable.Set

import org.objectweb.asm.{ClassReader, ClassVisitor, MethodVisitor, Type}
import org.objectweb.asm.Opcodes._
import java.io._
import java.lang.reflect.Modifier

object ClosureCleaner {
  // Get an ASM class reader for a given class from the JAR that loaded it
  private def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    // todo: Fixme - continuing with earlier behavior ...
    if (resourceStream == null) return new ClassReader(resourceStream)

    val baos = new ByteArrayOutputStream(128)
    copyStream(resourceStream, baos, true)
    new ClassReader(new ByteArrayInputStream(baos.toByteArray))
  }

  // Check whether a class represents a Scala closure
  private def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }

  // Get a list of the classes of the outer objects of a given closure object, obj;
  // the outer objects are defined as any closures that obj is nested within, plus
  // possibly the class that the outermost closure is in, if any. We stop searching
  // for outer objects beyond that because cloning the user's object is probably
  // not a good idea (whereas we can clone closure objects just fine since we
  // understand how all their fields are used).
  private def getOuterClasses(obj: AnyRef): List[Class[_]] = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      if (isClosure(f.getType)) {
        return f.getType :: getOuterClasses(f.get(obj))
      } else {
        return f.getType :: Nil // Stop at the first $outer that is not a closure
      }
    }
    return Nil
  }

  // Get a list of the outer objects for a given closure object.
  private def getOuterObjects(obj: AnyRef): List[AnyRef] = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      if (isClosure(f.getType)) {
        return f.get(obj) :: getOuterObjects(f.get(obj))
      } else {
        return f.get(obj) :: Nil // Stop at the first $outer that is not a closure
      }
    }
    return Nil
  }

  private def getInnerClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    var stack = List[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.head)
      stack = stack.tail
      val set = Set[Class[_]]()
      cr.accept(new InnerClosureFinder(set), 0)
      for (cls <- set -- seen) {
        seen += cls
        stack = cls :: stack
      }
    }
    return (seen - obj.getClass).toList
  }

  private def createNullValue(cls: Class[_]): AnyRef = {
    if (cls.isPrimitive) {
      new java.lang.Byte(0: Byte) // Should be convertible to any primitive type
    } else {
      null
    }
  }

  def clean[F <: AnyRef](func: F): F = {
    // TODO: cache outerClasses / innerClasses / accessedFields
    val outerClasses = getOuterClasses(func)
    val innerClasses = getInnerClasses(func)
    val outerObjects = getOuterObjects(func)
    val accessedFields = Map[Class[_], Set[String]]()
    for (cls <- func.getClass :: outerClasses)
      accessedFields(cls) = Set[String]()
    for (cls <- func.getClass :: innerClasses)
      getClassReader(cls).accept(new FieldAccessFinder(accessedFields), 0)

    // Nullify all the fields that are not used, keep $outer, though.
    // Also, do not mess with the first outer that is not a closure, this
    // is mostly the outer class/object of the user.
    var outer: AnyRef = func
    while (outer != null && isClosure(outer.getClass)) {
      //      outer = instantiateClass(cls, outer, inInterpreter)
      val cls = outer.getClass
      val newOuter = cls.getDeclaredFields.toList.find( _.getName == "$outer")
        .map { f => f.setAccessible(true); if (f.get(outer) != null) f.get(outer) else null }
        .getOrElse(null)

      for (field <- cls.getDeclaredFields if !accessedFields(cls).contains(field.getName) &&
        !Modifier.isStatic(field.getModifiers) && field.getName != "$outer") {
        field.setAccessible(true)
        if (field.get(outer) != null) {
          field.set(outer, null)
        }
      }
//      // when newOuter is the first non-closure, remove the outer pointer,
//      // this would mostly be the outer user code object/class
//      if (newOuter != null && !isClosure(newOuter.getClass)) {
//        for (field <- cls.getDeclaredFields if !accessedFields(cls).contains(field.getName) &&
//          !Modifier.isStatic(field.getModifiers)) {
//          field.setAccessible(true)
//          if (field.get(outer) != null) {
//            field.set(outer, null)
//          }
//        }
//      }
      outer = newOuter
    }
    func
  }

  /** Copy all data from an InputStream to an OutputStream */
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false)
  {
    val buf = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = in.read(buf)
      if (n != -1) {
        out.write(buf, 0, n)
      }
    }
    if (closeStreams) {
      in.close()
      out.close()
    }
  }
}

class FieldAccessFinder(output: Map[Class[_], Set[String]]) extends ClassVisitor(ASM4) {
  override def visitMethod(access: Int, name: String, desc: String,
                           sig: String, exceptions: Array[String]): MethodVisitor = {
    return new MethodVisitor(ASM4) {
      override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
        if (op == GETFIELD) {
          for (cl <- output.keys if cl.getName == owner.replace('/', '.')) {
            output(cl) += name
          }
        }
      }

      override def visitMethodInsn(op: Int, owner: String, name: String,
                                   desc: String) {
        // Check for calls a getter method for a variable in an interpreter wrapper object.
        // This means that the corresponding field will be accessed, so we should save it.
        if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
          for (cl <- output.keys if cl.getName == owner.replace('/', '.')) {
            output(cl) += name
          }
        }
      }
    }
  }
}

class InnerClosureFinder(output: Set[Class[_]]) extends ClassVisitor(ASM4) {
  var myName: String = null

  override def visit(version: Int, access: Int, name: String, sig: String,
                     superName: String, interfaces: Array[String]) {
    myName = name
  }

  override def visitMethod(access: Int, name: String, desc: String,
                           sig: String, exceptions: Array[String]): MethodVisitor = {
    return new MethodVisitor(ASM4) {
      override def visitMethodInsn(op: Int, owner: String, name: String,
                                   desc: String) {
        val argTypes = Type.getArgumentTypes(desc)
        if (op == INVOKESPECIAL && name == "<init>" && argTypes.length > 0
          && argTypes(0).toString.startsWith("L") // is it an object?
          && argTypes(0).getInternalName == myName)
          output += Class.forName(
            owner.replace('/', '.'),
            false,
            Thread.currentThread.getContextClassLoader)
      }
    }
  }
}

