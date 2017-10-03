/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Scala

class GreetingInScala {
    def greet() {
        val delegate = new GreetingInJava
        delegate.greet()
    }
}