<!--
This is a template for TiDB's change proposal process, documented [here](./README.md).
-->

# Proposal: <!-- Title -->

- Author(s):     <!-- Author Name, Co-Author Name, with the link(s) of the GitHub profile page --> 鄢程鹏、蔡涛
- Discussion at: <!-- https://github.com/pingcap/tidb/issues/XXX --> https://github.com/pingcap/tidb/issues/9245

## Abstract

<!--
A short summary of the proposal:
- What is the issue that the proposal aims to solve?
- What needs to be done in this proposal?
- What is the impact of this proposal?
-->
- 设计一个新的表达式求值的框架，使得表达式以向量为求值的对象，而非以行为求值的对象，减少表达式计算的开销，提高表达式计算的效率

## Background

<!--
An introduction of the necessary background and the problem being solved by the proposed change:

- The drawback of the current feature and the corresponding use case
- The expected outcome of this proposal.
-->
- 在当前的tidb版本中，表达式是按行进行求值计算的。对于输入中传入的一个chunk，需要进行多次函数调用才能确定一个chunk中每行的表达式计算后的结果，非常低效。我们可以拓展表达式求值的框架，增加以列为单位的表达式求值函数。做到对于一个传入的chunk，只需要进行一次函数调用，即可求得一个chunk中表达式的值。最终希望达到的效果是通过这种方式使得表达式求值更加高效、快速。

## Proposal

<!--
A precise statement of the proposed change:
- The new named concepts and a set of metrics to be collected in this proposal (if applicable)

- The overview of the design.

- How it works?

- What needs to be changed to implement this design?

- What may be positively influenced by the proposed change?

- What may be negatively impacted by the proposed change?
  -->

  我们此次的项目内容是需要对现有的表达式求值框架进行改进。现有的表达式求值接口expression在“tidb/expression”包中，在现有的接口中所有已经定义的表达式求值函数EvalXX(XX为表达式求值结果的返回值类型)都是以行为单位对表达式进行处理的。我们需要为其添加以向量为单位进行表达式计算的函数来完善其表达式计算的框架。

  

  首先，我们需要增加以向量为单位进行表达式求值的函数来完善当前的表达式求值框架，我们需要在原来的expression接口上定义VectorizedEvalXX函数。不仅如此，我们还需要定义一个类型为unsafe.Pointer的数据结构vector.Vector，它将作为VectorizedEvalXX的参数，用于保存在表达式计算过程中所得到的结果。

  

  接着，当在expression接口中添加了以向量为单位进行表达式计算的函数后，我们还需要为原有的内置表达式求值函数增加其向量化的实现版本。在本次的项目中，我们会首先考虑实现整型的算术运算。优先为那些整型的算术运算实现其向量化表达式计算的版本。其次才会考虑实现其他类型的表达式计算函数。

  

  最后，在具体的表达式求值函数的实现上，我们需要尽量最大化的利用好机器的性能来加速以向量为单位的表达式计算。其中可能采取的方法有使用SIMD指令，在一个CPU时钟周期内同时处理多个数据，减少总的处理时间。设计高效的数据结构，来提高cache的命中率。以及尽量复用vector.Vector，来减少GC的开销等一系列的操作。



## Rationale

<!--
A discussion of alternate approaches and the trade-offs, advantages, and disadvantages of the specified approach:
- How other systems solve the same issue?
- What other designs have been considered and what are their disadvantages?
- What is the advantage of this design compared with other designs?
- What is the disadvantage of this design?
- What is the impact of not doing this?
-->

Advantages:

* 提高了cache命中率。原来的版本中表达式的计算是以行为单位的，然而数据却是以列保存的，这样对一行进行表达式求值时，可能会出现cache命中率不高的情况。当以列为单位进行表达式计算时，可以在一定程度上提高cache的命中率。
* 减少了函数调用的开销。在原来的表达式计算框架中，对传入的chunk，对chunk中的每一行都需要进行一次函数调用。而在改进后的版本中，可以直接一次性对一个chunk进行整体的表达式计算，大大减少了函数调用的开销。
* 提高了表达式计算的速度。以列为对象进行表达式计算比以行为对象进行表达式计算有更多优化的可能，如SIMD指令可以在一个CPU周期内对多个数据对象同时进行计算，大大提高了计算的效率。



Disadvantages：

* 增加了GC的开销，因为进行改进后的框架中。表达式的计算是以向量为单位的，每次表达式计算的结果都需要保存进向量中返回给调用的函数。而创建向量所带来的GC开销是不可避免的。

## Test

- 对一条SQL语句，在不同表达式求值框架的实现下所用的时间进行比较
- 对于相同表达式求值类型（如整数加法），求出在不同实现（原来的版本和我们进行改进的版本）下表达式求值函数所用的时间

## Compatibility

<!--
A discussion of the change with regard to the compatibility issues:

- Does this proposal make TiDB not compatible with the old versions?
- Does this proposal make TiDB more compatible with MySQL?
  -->

## Implementation

<!--
A detailed description for each step in the implementation:

- Does any former steps block this step?
- Who will do it?
- When to do it?
- How long it takes to accomplish it?
-->

## Open issues (if applicable)

<!--
A discussion of issues relating to this proposal for which the author does not know the solution. This section may be omitted if there are none.
-->