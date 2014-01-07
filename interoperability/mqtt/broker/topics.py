"""
*******************************************************************
  Copyright (c) 2013, 2014 IBM Corp.
 
  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v1.0
  and Eclipse Distribution License v1.0 which accompany this distribution. 
 
  The Eclipse Public License is available at 
     http://www.eclipse.org/legal/epl-v10.html
  and the Eclipse Distribution License is available at 
    http://www.eclipse.org/org/documents/edl-v10.php.
 
  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************
"""


import re, logging

 
def isValidTopicName(aName):
  rc = True

  # '#' wildcard can be only at the beginning or the end of a topic
  if aName[1:-1].find('#') != -1:
    rc = False

  # '#' or '+' only next to a slash separator or end of name
  wilds = '#+'
  for c in wilds:
    pos = 0
    pos = aName.find(c, pos)
    while pos != -1:
      if pos > 0: # check previous char is '/'
        if aName[pos-1] != '/':
          rc = False
      if pos < len(aName)-1: # check that subsequent char is '/'
        if aName[pos+1] != '/':
          rc = False
      pos = aName.find(c, pos+1)

  return rc
 
def topicMatches(wild, nonwild, wildCheck=True):

  if wildCheck:
    assert nonwild.find('+') == nonwild.find('#') == -1
  assert isValidTopicName(wild) and isValidTopicName(nonwild)

  rc = False
  if wild.find('+') == wild.find('#') == -1:
    # no wildcards, so check is simple
    rc = (wild == nonwild)
  else:
    # we have wildcards. Escape metacharacters, except +
    metachars = '\\.^$*?{[]|()' # make sure \\ is at beginning of this string
    for s in metachars:
      wild = wild.replace(s, '\\'+s)

    # now replace wildcards with regular expressions and match
    if wild == '#':
      "# on its own matches everything"
      wild = '.*'
    elif wild == '/#':
      "/# matches everything starting with a slash"
      wild = '/.*'
    else:
      "all other instances subsume the preceding or following slash"
      wild = wild.replace('#/', '(.*?/|^)').replace('/#', '(/.*?|$)')
    wild = wild.replace('+', '[^/#]+?') # + does not match an empty level
    if re.compile(wild+'$').match(nonwild) != None:
      rc = True
  return rc
 
def topicIncludes(topic1, topic2):
  "does topic1 subsume topic2, i.e. is topic2 a subset of topic1?"
  rc = False
  if topicMatches(topic1, topic2, False):
    rc = True
  return rc
 
def topicOverlaps(topic1, topic2):
  "is the intersection of the two topics non-null?"

  def reverse(string):
    l = list(string)
    l.reverse()
    return ''.join(l)

  def addto(topic, level):
    if topic == '':
      rc = level
    else:
      rc = topic+'/'+level
    return rc

  def test(topic1, topic2):
    # construct example for topic2
    global example
    example = ''
    topic1levels = topic1.split('/')
    for topic2level in topic2.split('/'):
      if topic2level == '+':
        if len(topic1levels) > 0 and topic1levels[0] not in '+#':
          level = topic1levels.pop(0)
        else:
          level = 'x'
        example = addto(example, level)
      elif topic2level != '#':
        example = addto(example, topic2level)
        if topic1levels[0] in [topic2level, '+']:
          topic1levels.pop(0)
    return topicMatches(topic1, example)

  rc = False
  if (topic1[0] == topic2[-1] == '#') or \
     (topic1[-1] == topic2[0] == '#'):
    rc = True
  elif topic1[0] == '#' or topic2[0] == '#':
    rc = test(reverse(topic1), reverse(topic2))
  else:
    rc = test(topic1, topic2)
  return rc
""" 
topicCoverage = dataclasses("string", (1, 64),
                ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA", "MQTTSAS topic"],
                static=True)
wildTopicCoverage = dataclasses("string", (1, 64),
                    ["TopicA/+", "+/C", "#", "/#", "/+", "+/+"],
                    static=True)
""" 
if __name__ == "__main__":
  topics = \
  ['level1', 'level1/level2', 'level1/level2/level3',
   'le(el1/le?el2', '/level1a']

  tests = \
  [('level1/+/level3', [False, False, True, False, False]),
   ('level1/#', [True, True, True, False, False]),
   ('level1/level2', [False, True, False, False, False]),
   ('le(el1/le)el2', [False, False, False, False, False]),
   ('+/le?el2', [False, False, False, True, False]),
   ('/le?el2', [False, False, False, False, False]),
   ('/+', [False, False, False, False, True]),
   ('#/level3', [False, False, True, False, False]),
   ('#/level1', [True, False, False, False, False]),
   ('/#', [False, False, False, False, True]),
   ('#', [True, True, True, True, True])]

  for t in tests:
    wildtopic, results = t
    assert isValidTopicName(wildtopic), \
               "Topic validation failed for: "+wildtopic
    for i in range(len(results)):
      assert topicMatches(wildtopic, topics[i]) == results[i], \
        "Failed: "+ wildtopic+" "+topics[i]+" "+repr(results[i])
      logging.info("Worked:", wildtopic, topics[i], results[i])

  tests = \
  [('level1/+/level3', True),
   ('level1//level3', True),
   ('level1/#/level3', False),
   ('#/level1/+/level4/#', True),
   ('##/level2', False),
   ('level1#', False)]

  for topic in topics:
    assert isValidTopicName(topic), "Topic validation failed for: "+topic
  for t in tests:
    topic, result = t
    assert isValidTopicName(topic) == result, "Topic validation failed for: "+topic

  itests = \
   [("#/a/+/+", "#/+/b/+"),
    ("#/a/+/+", "#/a/b/+"),
    ("#/a/+/+", "x/a/b/#")]


  for i in itests:
    def inc(t1, t2):
      logging.info(t1, "includes", t2, topicIncludes(t1, t2))
    inc(i[0], i[1])
    inc(i[1], i[0])

  otests = \
    [("#/x/+/z", "#/a/b/c/+/y/+", True),
     ("#/x/+/z", "a/b/c/+/y/#", True),
     ("+/x/d/z", "a/b/c/+/y/#", False),
     ("/#", "#/b/c/+/y", True),
     ("+/b/+", "a/+/c", True),
     ("+/b/d", "a/+/c", False),
     ("+/b/c", "a/+/c", True)]
  for o in otests:
    logging.info(o[0], "overlaps", o[1], topicOverlaps(o[0], o[1]))
    assert topicOverlaps(o[0], o[1]) == o[2]

  logging.info(not topicMatches("1/2/#", "1/2s/s"))
  assert not topicMatches("1/2/#", "1/2s/s")
  assert topicMatches("1/2/#", "1/2/s")

  assert topicMatches("#/1/2", "1/2")
  assert not topicMatches("#/1/2", "o/s1/2")

 
