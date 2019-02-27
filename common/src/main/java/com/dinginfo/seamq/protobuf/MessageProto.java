// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: MessageProto.proto

package com.dinginfo.seamq.protobuf;

public final class MessageProto {
  private MessageProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface MessageProOrBuilder extends
      // @@protoc_insertion_point(interface_extends:com.dinginfo.seamq.MessagePro)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional bytes data = 1;</code>
     */
    boolean hasData();
    /**
     * <code>optional bytes data = 1;</code>
     */
    com.google.protobuf.ByteString getData();

    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    java.util.List<com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro> 
        getAttributesList();
    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro getAttributes(int index);
    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    int getAttributesCount();
    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    java.util.List<? extends com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldProOrBuilder> 
        getAttributesOrBuilderList();
    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldProOrBuilder getAttributesOrBuilder(
        int index);
  }
  /**
   * Protobuf type {@code com.dinginfo.seamq.MessagePro}
   */
  public static final class MessagePro extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:com.dinginfo.seamq.MessagePro)
      MessageProOrBuilder {
    // Use MessagePro.newBuilder() to construct.
    private MessagePro(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private MessagePro(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final MessagePro defaultInstance;
    public static MessagePro getDefaultInstance() {
      return defaultInstance;
    }

    public MessagePro getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private MessagePro(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              data_ = input.readBytes();
              break;
            }
            case 18: {
              if (!((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
                attributes_ = new java.util.ArrayList<com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro>();
                mutable_bitField0_ |= 0x00000002;
              }
              attributes_.add(input.readMessage(com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.PARSER, extensionRegistry));
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
          attributes_ = java.util.Collections.unmodifiableList(attributes_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.dinginfo.seamq.protobuf.MessageProto.internal_static_com_dinginfo_seamq_MessagePro_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.dinginfo.seamq.protobuf.MessageProto.internal_static_com_dinginfo_seamq_MessagePro_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.dinginfo.seamq.protobuf.MessageProto.MessagePro.class, com.dinginfo.seamq.protobuf.MessageProto.MessagePro.Builder.class);
    }

    public static com.google.protobuf.Parser<MessagePro> PARSER =
        new com.google.protobuf.AbstractParser<MessagePro>() {
      public MessagePro parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new MessagePro(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<MessagePro> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    public static final int DATA_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString data_;
    /**
     * <code>optional bytes data = 1;</code>
     */
    public boolean hasData() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bytes data = 1;</code>
     */
    public com.google.protobuf.ByteString getData() {
      return data_;
    }

    public static final int ATTRIBUTES_FIELD_NUMBER = 2;
    private java.util.List<com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro> attributes_;
    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    public java.util.List<com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro> getAttributesList() {
      return attributes_;
    }
    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    public java.util.List<? extends com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldProOrBuilder> 
        getAttributesOrBuilderList() {
      return attributes_;
    }
    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    public int getAttributesCount() {
      return attributes_.size();
    }
    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    public com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro getAttributes(int index) {
      return attributes_.get(index);
    }
    /**
     * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
     */
    public com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldProOrBuilder getAttributesOrBuilder(
        int index) {
      return attributes_.get(index);
    }

    private void initFields() {
      data_ = com.google.protobuf.ByteString.EMPTY;
      attributes_ = java.util.Collections.emptyList();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, data_);
      }
      for (int i = 0; i < attributes_.size(); i++) {
        output.writeMessage(2, attributes_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, data_);
      }
      for (int i = 0; i < attributes_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, attributes_.get(i));
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.dinginfo.seamq.protobuf.MessageProto.MessagePro parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.dinginfo.seamq.protobuf.MessageProto.MessagePro prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.dinginfo.seamq.MessagePro}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:com.dinginfo.seamq.MessagePro)
        com.dinginfo.seamq.protobuf.MessageProto.MessageProOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.dinginfo.seamq.protobuf.MessageProto.internal_static_com_dinginfo_seamq_MessagePro_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.dinginfo.seamq.protobuf.MessageProto.internal_static_com_dinginfo_seamq_MessagePro_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.dinginfo.seamq.protobuf.MessageProto.MessagePro.class, com.dinginfo.seamq.protobuf.MessageProto.MessagePro.Builder.class);
      }

      // Construct using com.dinginfo.seamq.protobuf.MessageProto.MessagePro.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          getAttributesFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        data_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        if (attributesBuilder_ == null) {
          attributes_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000002);
        } else {
          attributesBuilder_.clear();
        }
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.dinginfo.seamq.protobuf.MessageProto.internal_static_com_dinginfo_seamq_MessagePro_descriptor;
      }

      public com.dinginfo.seamq.protobuf.MessageProto.MessagePro getDefaultInstanceForType() {
        return com.dinginfo.seamq.protobuf.MessageProto.MessagePro.getDefaultInstance();
      }

      public com.dinginfo.seamq.protobuf.MessageProto.MessagePro build() {
        com.dinginfo.seamq.protobuf.MessageProto.MessagePro result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.dinginfo.seamq.protobuf.MessageProto.MessagePro buildPartial() {
        com.dinginfo.seamq.protobuf.MessageProto.MessagePro result = new com.dinginfo.seamq.protobuf.MessageProto.MessagePro(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.data_ = data_;
        if (attributesBuilder_ == null) {
          if (((bitField0_ & 0x00000002) == 0x00000002)) {
            attributes_ = java.util.Collections.unmodifiableList(attributes_);
            bitField0_ = (bitField0_ & ~0x00000002);
          }
          result.attributes_ = attributes_;
        } else {
          result.attributes_ = attributesBuilder_.build();
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.dinginfo.seamq.protobuf.MessageProto.MessagePro) {
          return mergeFrom((com.dinginfo.seamq.protobuf.MessageProto.MessagePro)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.dinginfo.seamq.protobuf.MessageProto.MessagePro other) {
        if (other == com.dinginfo.seamq.protobuf.MessageProto.MessagePro.getDefaultInstance()) return this;
        if (other.hasData()) {
          setData(other.getData());
        }
        if (attributesBuilder_ == null) {
          if (!other.attributes_.isEmpty()) {
            if (attributes_.isEmpty()) {
              attributes_ = other.attributes_;
              bitField0_ = (bitField0_ & ~0x00000002);
            } else {
              ensureAttributesIsMutable();
              attributes_.addAll(other.attributes_);
            }
            onChanged();
          }
        } else {
          if (!other.attributes_.isEmpty()) {
            if (attributesBuilder_.isEmpty()) {
              attributesBuilder_.dispose();
              attributesBuilder_ = null;
              attributes_ = other.attributes_;
              bitField0_ = (bitField0_ & ~0x00000002);
              attributesBuilder_ = 
                com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders ?
                   getAttributesFieldBuilder() : null;
            } else {
              attributesBuilder_.addAllMessages(other.attributes_);
            }
          }
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.dinginfo.seamq.protobuf.MessageProto.MessagePro parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.dinginfo.seamq.protobuf.MessageProto.MessagePro) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private com.google.protobuf.ByteString data_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes data = 1;</code>
       */
      public boolean hasData() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional bytes data = 1;</code>
       */
      public com.google.protobuf.ByteString getData() {
        return data_;
      }
      /**
       * <code>optional bytes data = 1;</code>
       */
      public Builder setData(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        data_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes data = 1;</code>
       */
      public Builder clearData() {
        bitField0_ = (bitField0_ & ~0x00000001);
        data_ = getDefaultInstance().getData();
        onChanged();
        return this;
      }

      private java.util.List<com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro> attributes_ =
        java.util.Collections.emptyList();
      private void ensureAttributesIsMutable() {
        if (!((bitField0_ & 0x00000002) == 0x00000002)) {
          attributes_ = new java.util.ArrayList<com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro>(attributes_);
          bitField0_ |= 0x00000002;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilder<
          com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldProOrBuilder> attributesBuilder_;

      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public java.util.List<com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro> getAttributesList() {
        if (attributesBuilder_ == null) {
          return java.util.Collections.unmodifiableList(attributes_);
        } else {
          return attributesBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public int getAttributesCount() {
        if (attributesBuilder_ == null) {
          return attributes_.size();
        } else {
          return attributesBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro getAttributes(int index) {
        if (attributesBuilder_ == null) {
          return attributes_.get(index);
        } else {
          return attributesBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public Builder setAttributes(
          int index, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro value) {
        if (attributesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureAttributesIsMutable();
          attributes_.set(index, value);
          onChanged();
        } else {
          attributesBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public Builder setAttributes(
          int index, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder builderForValue) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          attributes_.set(index, builderForValue.build());
          onChanged();
        } else {
          attributesBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public Builder addAttributes(com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro value) {
        if (attributesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureAttributesIsMutable();
          attributes_.add(value);
          onChanged();
        } else {
          attributesBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public Builder addAttributes(
          int index, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro value) {
        if (attributesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureAttributesIsMutable();
          attributes_.add(index, value);
          onChanged();
        } else {
          attributesBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public Builder addAttributes(
          com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder builderForValue) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          attributes_.add(builderForValue.build());
          onChanged();
        } else {
          attributesBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public Builder addAttributes(
          int index, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder builderForValue) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          attributes_.add(index, builderForValue.build());
          onChanged();
        } else {
          attributesBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public Builder addAllAttributes(
          java.lang.Iterable<? extends com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro> values) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, attributes_);
          onChanged();
        } else {
          attributesBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public Builder clearAttributes() {
        if (attributesBuilder_ == null) {
          attributes_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000002);
          onChanged();
        } else {
          attributesBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public Builder removeAttributes(int index) {
        if (attributesBuilder_ == null) {
          ensureAttributesIsMutable();
          attributes_.remove(index);
          onChanged();
        } else {
          attributesBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder getAttributesBuilder(
          int index) {
        return getAttributesFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldProOrBuilder getAttributesOrBuilder(
          int index) {
        if (attributesBuilder_ == null) {
          return attributes_.get(index);  } else {
          return attributesBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public java.util.List<? extends com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldProOrBuilder> 
           getAttributesOrBuilderList() {
        if (attributesBuilder_ != null) {
          return attributesBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(attributes_);
        }
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder addAttributesBuilder() {
        return getAttributesFieldBuilder().addBuilder(
            com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.getDefaultInstance());
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder addAttributesBuilder(
          int index) {
        return getAttributesFieldBuilder().addBuilder(
            index, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.getDefaultInstance());
      }
      /**
       * <code>repeated .com.dinginfo.seamq.DataFieldPro attributes = 2;</code>
       */
      public java.util.List<com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder> 
           getAttributesBuilderList() {
        return getAttributesFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilder<
          com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldProOrBuilder> 
          getAttributesFieldBuilder() {
        if (attributesBuilder_ == null) {
          attributesBuilder_ = new com.google.protobuf.RepeatedFieldBuilder<
              com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldPro.Builder, com.dinginfo.seamq.protobuf.DataFieldProto.DataFieldProOrBuilder>(
                  attributes_,
                  ((bitField0_ & 0x00000002) == 0x00000002),
                  getParentForChildren(),
                  isClean());
          attributes_ = null;
        }
        return attributesBuilder_;
      }

      // @@protoc_insertion_point(builder_scope:com.dinginfo.seamq.MessagePro)
    }

    static {
      defaultInstance = new MessagePro(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.dinginfo.seamq.MessagePro)
  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_dinginfo_seamq_MessagePro_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_dinginfo_seamq_MessagePro_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\022MessageProto.proto\022\022com.dinginfo.seamq" +
      "\032\024DataFieldProto.proto\"P\n\nMessagePro\022\014\n\004" +
      "data\030\001 \001(\014\0224\n\nattributes\030\002 \003(\0132 .com.din" +
      "ginfo.seamq.DataFieldProB+\n\033com.dinginfo" +
      ".seamq.protobufB\014MessageProto"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.dinginfo.seamq.protobuf.DataFieldProto.getDescriptor(),
        }, assigner);
    internal_static_com_dinginfo_seamq_MessagePro_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_com_dinginfo_seamq_MessagePro_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_com_dinginfo_seamq_MessagePro_descriptor,
        new java.lang.String[] { "Data", "Attributes", });
    com.dinginfo.seamq.protobuf.DataFieldProto.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
