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

  我们此次的项目内容是需要对现有的表达式求值框架进行改进。现有的表达式求值接口expression位于“tidb/expression”包中，在expression接口现有的求值函数中，所有已经定义的表达式求值函数EvalXX(XX为表达式求值结果的返回值类型)都是以行为单位对表达式进行处理的。我们需要为其添加以向量为单位进行表达式计算的函数来完善其表达式计算的框架，从而可以达到，对于每次通过到达projection算子的chunk，经过一次表达式求值即可得到结果，而不是像原来框架中对给定chunk的每一行都调用一次表达式求值的函数。

  

  首先，我们需要增加以向量为单位进行表达式求值的函数来完善当前的表达式求值框架，我们需要在原来的expression接口上定义ColEvalXX函数，它与原来EvalXX函数的不同之处在于，前者一次处理一个chunk，直接得到一个chunk中所有(逻辑)行表达式计算的结果，而后者一次只处理一行，它需要通过多次调用表达式求值函数才能得到对应chunk表达式求值后的结果。不仅如此，我们还需要定义一个能够保存ColEvalXX函数计算所得结果的数据结构，它应该足够通用，能表示所有的类型，并且它还需要足够紧凑，尽量节省内存。因此我们选择原本就存在于“tidb/util/chunk”包中的的column结构。

  

  接着，当在expression接口中添加了以向量为单位进行表达式计算的函数后，我们还需要为原有的内置表达式求值函数增加其向量化的实现版本。在本次的项目中，我们会考虑实现不同的类型的不同表达式求值函数。我们在expression接口中添加了int, real, decimal, string的向量化表达式求值函数。在此基础上，我们为不同的类型实现了不同的表达式，我们为int实现了加法，为real实现了乘法，为decimal实现了greatest函数，为string实现了concat函数。

  

  最后，在具体的表达式求值函数的实现上，我们需要尽量利用好机器的性能，在正确性能够保证的前提下尽量减少不计算，如对Null的处理，在多参数的函数中可以提前保存null的值，这样在计算的过程可以直接取用提前保存的null的值，而不是每次重新函数调用获取。一些data也可以用类似的方法提前存到临时变量中，以此加快获取的速度。另外，对于column结构，我们可以尽量复用以减少GC的开销，如在多参数的表达式计算中，我们可以将现有的column结构不断复用，作为表达式计算的参数重复使用。



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

* 提高了cache命中率。原来的版本中表达式的计算是以行为单位的，然而数据却是以列保存的，这样对一行进行表达式求值时，可能会出现cache命中率不高的情况。当以列为单位进行表达式计算时，一列中的数据可以尽量长的呆在cache中，可以在一定程度上提高cache的命中率。于此同时，作为保存中间结果的column结构，其本身十分紧凑，没有过多额外的内存开销。
* 减少了函数调用的开销。在原来的表达式计算框架中，对传入的chunk，对chunk中的每一行都需要进行一次函数调用。而在改进后的版本中，可以直接一次性对一个chunk进行整体的表达式计算，大大减少了函数调用的开销。
* 提高了表达式计算的速度。以列为对象进行表达式计算比以行为对象进行表达式计算有更多优化的可能，如当数据以列为单位进行计算时，我们可以增加数据以及指令的并行度来提高表达式计算的速度，从而获得较好的性能。



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