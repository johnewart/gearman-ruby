#!/usr/bin/env ruby

$:.unshift('../lib')
require 'gearman'
require 'gearman/testlib'
require 'test/unit'

class TestUtil < Test::Unit::TestCase

  def test_ability_prefix_name_builder
    assert_equal(Gearman::Util.ability_name_with_prefix("test","a"),"test\ta")
  end

  def test_ability_name_for_perl
    assert_equal(Gearman::Util.ability_name_for_perl("test","a"),"test\ta")
  end  
end
