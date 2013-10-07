/*
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * 
 * @author Larry Lindsey llindsey@clm.utexas.edu
 */

package edu.utexas.clm.archipelago.network;

import edu.utexas.clm.archipelago.FijiArchipelago;
import edu.utexas.clm.archipelago.compute.ProcessManager;
import edu.utexas.clm.archipelago.data.ClusterMessage;
import edu.utexas.clm.archipelago.listen.MessageType;
import edu.utexas.clm.archipelago.listen.TransceiverExceptionListener;
import edu.utexas.clm.archipelago.listen.TransceiverListener;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import edu.utexas.clm.archipelago.network.translation.Bottler;

import java.io.*;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Message transceiver class
 */
public class MessageXC
{

    private class FileTranslatingInputStream extends ObjectInputStream
    {
        public FileTranslatingInputStream(final InputStream is) throws IOException
        {
            super(is);
            enableResolveObject(true);
        }

        private Object unBottle(final Object object)
        {
            if (object instanceof Bottle)
            {
                Bottle bottle = (Bottle)object;
                return bottle.unBottle();
            }
            else
            {
                return object;
            }
        }

        protected final Object resolveObject(final Object object)
        {
            final Object resolved = unBottle(object);

            if (resolved instanceof File)
            {
                final File file = (File)object;
                return translateFile(file, false);
            }
            else
            {
                return resolved;
            }
        }
    }

    private class FileTranslatingOutputStream extends ObjectOutputStream
    {

        public FileTranslatingOutputStream(final OutputStream os) throws IOException
        {
            super(os);
            enableReplaceObject(true);
        }

        private Object bottle(final Object object)
        {
            ArrayList<Bottler> localBottlers = new ArrayList<Bottler>(bottlers);
            for (final Bottler bottler : localBottlers)
            {

                if (bottler.accepts(object))
                {
                    return bottler.bottle(object);
                }
            }
            return object;
        }


        protected final Object replaceObject(final Object object)
        {
            final Object replacement = bottle(object);

            if (replacement instanceof File)
            {
                final File file = (File)object;
                return translateFile(file, true);
            }
            else
            {
                return replacement;
            }

        }
    }

    private class RXThread extends Thread
    {
        public void run()
        {
            while (active.get())
            {
                try
                {
                    ClusterMessage message = (ClusterMessage)objectInputStream.readObject();
                    // Don't debug beats, or they'll fill your log
                    if (message.type != MessageType.BEAT)
                    {
                        FijiArchipelago.debug("RX: " + id + " got message " +
                                ClusterMessage.messageToString(message));
                        if (message.type == MessageType.PROCESS)
                        {
                            ProcessManager pm = (ProcessManager)message.o;
                            FijiArchipelago.debug("RX: Got message for job " + pm.getID());
                        }
                    }
                    xcListener.handleMessage(message);
                    objectInputStream = new FileTranslatingInputStream(inStream);
                }
                catch (ClassCastException cce)
                {
                    xcExceptionListener.handleRXThrowable(cce, xc);
                }
                catch (IOException ioe)
                {
                    xcExceptionListener.handleRXThrowable(ioe, xc);
                }
                catch (ClassNotFoundException cnfe)
                {
                    xcExceptionListener.handleRXThrowable(cnfe, xc);
                }
                catch (Exception e)
                {
                    xcExceptionListener.handleRXThrowable(e, xc);
                }
            }
        }
    }
    
    private class TXThread extends Thread
    {
        public void run()
        {
            while (active.get())
            {
                ClusterMessage nextMessage = null;
                try
                {
                    nextMessage = messageQ.poll(waitTime, tUnit);
                }
                catch (InterruptedException ie)
                {
                    active.set(false);
                }

                if (nextMessage != null)
                {
                    try
                    {
                        if (nextMessage.type != MessageType.BEAT)
                        {
                            FijiArchipelago.debug("TX: " + id + " writing message " +
                                    ClusterMessage.messageToString(nextMessage));
                        }
                        objectOutputStream.writeObject(nextMessage);
                        objectOutputStream.flush();
                        objectOutputStream = new FileTranslatingOutputStream(outStream);

                    }
                    catch (NotSerializableException nse)
                    {
                        xcExceptionListener.handleTXThrowable(nse, xc);
                    }
                    catch (IOException ioe)
                    {
                        xcExceptionListener.handleTXThrowable(ioe, xc);
                    }
                    catch (ConcurrentModificationException ccme)
                    {
                        xcExceptionListener.handleTXThrowable(ccme, xc);
                    }
                    catch (RuntimeException re)
                    {
                        xcExceptionListener.handleTXThrowable(re, xc);
                    }
                    catch (Exception e)
                    {
                        xcExceptionListener.handleTXThrowable(e, xc);
                    }
                }
            }
        }
    }
    
    public static final long DEFAULT_WAIT = 10000;
    public static final TimeUnit DEFAULT_UNIT = TimeUnit.MILLISECONDS;

    private final Vector<Bottler> bottlers;
    private final ArrayBlockingQueue<ClusterMessage> messageQ;
    private FileTranslatingOutputStream objectOutputStream;
    private FileTranslatingInputStream objectInputStream;
    private final Thread txThread, rxThread;
    private final AtomicBoolean active, enableFileTranslation;
    private final long waitTime;
    private final TimeUnit tUnit;
    private final TransceiverListener xcListener;
    private final TransceiverExceptionListener xcExceptionListener;
    private final OutputStream outStream;
    private final InputStream inStream;
    private long id;

    private String localFileRoot, remoteFileRoot;

    private final MessageXC xc = this;

    public MessageXC(final InputStream inStream,
                     final OutputStream outStream,
                     final TransceiverListener listener,
                     final TransceiverExceptionListener listenerE) throws IOException
    {
        this(inStream, outStream, listener, listenerE, DEFAULT_WAIT, DEFAULT_UNIT);
    }

    public MessageXC(InputStream inStream,
                     OutputStream outStream,
                     final TransceiverListener listener,
                     final TransceiverExceptionListener listenerE,
                     final long wait,
                     TimeUnit unit) throws IOException
    {
        FijiArchipelago.debug("Creating Message Transciever");
        enableFileTranslation = new AtomicBoolean(true);
        bottlers = new Vector<Bottler>();
        messageQ = new ArrayBlockingQueue<ClusterMessage>(16, true);
        objectOutputStream = new FileTranslatingOutputStream(outStream);
        objectInputStream =  new FileTranslatingInputStream(inStream);
        FijiArchipelago.debug("XC: streams are set");
        this.inStream = inStream;
        this.outStream = outStream;
        active = new AtomicBoolean(true);
        waitTime = wait;
        tUnit = unit;
        xcListener = listener;
        xcExceptionListener = listenerE;
        localFileRoot = "";
        remoteFileRoot = "";

        txThread = new TXThread();
        rxThread = new RXThread();

        id = -1;

        rxThread.start();
        txThread.start();
    }

    public void close()
    {
        if (active.get())
        {
            FijiArchipelago.debug("XC: Got close.");
            active.set(false);
            xcListener.streamClosed();
            txThread.interrupt();
            rxThread.interrupt();
            try
            {
                inStream.close();
            }
            catch (IOException ioe) {/**/}
            try
            {
                outStream.close();
            }
            catch (IOException ioe) {/**/}
        }
    }

    public boolean join()
    {
        if (txThread.isAlive() || rxThread.isAlive())
        {
            try
            {
                txThread.join();
                rxThread.join();
                return true;
            }
            catch (InterruptedException ie)
            {
                return false;
            }
        }
        else
        {
            return true;
        }
    }

    public synchronized boolean queueMessage(ClusterMessage message)
    {
        try
        {
            messageQ.put(message);
            return true;
        }
        catch (InterruptedException ie)
        {
            return false;
        }
    }

    public boolean queueMessage(final MessageType type)
    {
        ClusterMessage cm = new ClusterMessage(type);
        return queueMessage(cm);
    }

    public boolean queueMessage(final MessageType type, final Serializable o)
    {
        ClusterMessage cm = new ClusterMessage(type);
        cm.o = o;
        return queueMessage(cm);
    }

    public long getId()
    {
        return id;
    }

    public void setId(final long id)
    {
        this.id = id;
    }

    private File translateHelper(final File file, final String fromPath, final String toPath)
    {
        final String filePath = file.getAbsolutePath();
        if (filePath.startsWith(fromPath))
        {
            return new File(filePath.replace(fromPath, toPath));
        }
        else
        {
            return file;
        }
    }

    public File translateFile(final File file,
                              boolean directionLocalRemote)
    {
        if (enableFileTranslation.get())
        {
            return directionLocalRemote ? translateHelper(file, localFileRoot, remoteFileRoot) :
                    translateHelper(file, remoteFileRoot, localFileRoot);
        }
        else
        {
            return file;
        }
    }

    public void setFileSystemTranslation(final String local, final String remote)
    {
        if (local.endsWith("/") || local.endsWith("\\"))
        {
            localFileRoot = local.substring(0, local.length() - 1);
        }
        else
        {
            localFileRoot = local;
        }

        if (remote.endsWith("/") || remote.endsWith("\\"))
        {
            remoteFileRoot = remote.substring(0, remote.length() - 1);
        }
        else
        {
            remoteFileRoot = remote;
        }

        enableFileTranslation.set(true);
    }

    public void unsetFileSystemTranslation()
    {
        enableFileTranslation.set(false);
    }

    public void addBottler(final Bottler bottler)
    {
        bottlers.add(bottler);
    }

    public Vector<Bottler> getBottlers()
    {
        return bottlers;
    }
}
